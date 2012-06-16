var Stream = require('stream').Stream
  , request = require('request')
  , inherits = require('util').inherits
  , querystring = require('querystring')
  , SAXParser = require('sax').SAXParser


function S3Error(data) {
  Error.captureStackTrace(this, S3Error)
  this.code = data.Code
  this.name = 'S3' + data.Code
  this.message = data.Message
  this.bucketName = data.BucketName
  this.requestId = data.RequestId
  this.hostId = HostId
}
inherits(S3Error, Error)
exports.S3Error = S3Error


function ParseError(message) {
  Error.captureStackTrace(this, ParseError)
  this.name = 'ParseError'
  this.message = message
}
inherits(ParseError, Error)
exports.ParseError = ParseError


exports = module.exports = function(opts) {
  opts = opts || {}

  var stream = new S3ListStream()
    , remaining = opts['max-keys'] || opts.maxkeys || Infinity
    , received = 0
    , secure = typeof opts.secure == 'undefined' ? true : opts.secure
    , aws = { key: opts.key
            , secret: opts.secret
            , bucket: opts.bucket
            }
    , qs =  {}

  if (opts.delimiter) qs.delimiter = opts.delimiter
  if (opts.prefix) qs.prefix = opts.prefix

  function onerror(err) {
    stream.emit('error', err)
  }
  function onfile(file) {
    stream.emit('data', file)
  }
  function onend() {
    remaining -= this.received

    if (this.meta.CommonPrefixes) {
      stream.commonPrefixes.push.apply(stream.commonPrefixes, this.meta.CommonPrefixes)
    }

    if (this.meta.IsTruncated && remaining) {
      stream.emit('progress')
      execute(this.meta.NextMarker || this.lastFile.Key)
      return
    }

    stream.name = this.meta.Name
    stream.delimiter = this.meta.Delimiter

    this.readable = false
    stream.emit('end')
  }

  function execute(marker) {
    if (marker) qs.marker = marker
    if (remaining < 1000) qs['max-keys'] = remaining

    var uri = 'http'
            + (secure ? 's' : '')
            + '://'
            + aws.bucket
            + '.s3.amazonaws.com'
            + '/?'
            + querystring.stringify(qs)

    var req = request(uri)
    req.aws(aws)

    var parser = new Parser(req)
    req.pipe(parser)
    stream.parser = parser

    req.on('error', onerror)
    parser.on('error', onerror)

    parser.on('file', onfile)
    parser.on('end', onend)

  }

  execute()

  return stream
}

function S3ListStream() {
  Stream.call(this)
  this.readable = true
  this.commonPrefixes = []
}
inherits(S3ListStream, Stream)
exports.S3ListStream = S3ListStream

;['pause', 'resume', 'destroy'].forEach(function(key) {
  S3ListStream.prototype[key] = function() {
    if (key === 'destroy') this.readable = false

    if (!this.parser) return
    this.parser[key]()
  }
})




function Parser(request) {
  Stream.call(this)

  var parser = new SAXParser(true)
    , meta = {}
    , state = 'start'
    , self = this
    , obj
    , key


  this.request = request

  this.writable = true
  this.paused = false

  this.parser = parser
  this.meta = meta

  this.received = 0


  parser.onopentag = function(info) {
    key = info.name
    switch(state) {
    case 'start':
      if (key == 'Error') {
        state = 'error'
        obj = {}
      } else {
        state = 'meta'
      }
      break

    case 'meta':
      if (key == 'Contents') {
        state = 'file'
        obj = {}
      } else if (key == 'CommonPrefixes') {
        state = 'prefixes'
      }
      break

    case 'file':
      if (key == 'Owner') {
        state = 'owner'
        obj.Owner = {}
      }
      break

    case 'error':
    case 'prefixes':
    case 'owner':
      break

    default:
      //self.emit('error', new Error('Unknown open tag; ' + key))
      break

    }
  }

  parser.onclosetag = function(name) {
    switch(state) {
    case 'error':
      if (name == 'Error') {
        self.emit('error', obj)
      }
      break

    case 'owner':
      if (name == 'Owner') {
        state = 'file'
      }
      break

    case 'file':
      if (name == 'Contents') {
        self.received++
        self.emit('file', obj)
        state = 'meta'
      }
      break

    case 'prefixes':
      if (name == 'CommonPrefixes') {
        state = 'meta'
      }
      break

    case 'meta': // close ListBucketResult
      break

    default:
      //self.emit('error', new Error('Unknown close tag; ' + name))
      break

    }
  }
  parser.ontext = function(text) {
    switch(state + ':' + key) {
    case 'meta:Delimiter':
    case 'meta:Name':
    case 'meta:Marker':
    case 'meta:NextMarker':
      meta[key] = text
      break

    case 'meta:MaxKeys':
      meta[key] = Number(text)
      break

    case 'meta:IsTruncated':
      meta[key] = text == 'true' ? true : false
      break

    case 'prefixes:Prefix':
      if (!meta.CommonPrefixes) meta.CommonPrefixes = []
      meta.CommonPrefixes.push(text)
      break

    case 'error:Code':
    case 'error:Message':
    case 'error:BucketName':
    case 'error:RequestId':
    case 'error:HostId':

    case 'file:Key':
    case 'file:ETag':
    case 'file:StorageClass':
      obj[key] = text
      break

    case 'file:Size':
      obj[key] = Number(text)
      break

    case 'file:LastModified':
      obj[key] = new Date(text)
      break

    case 'owner:DisplayName':
    case 'owner:ID':
      obj.Owner[key] = text
      break

    }
  }

  parser.onend = function() {
    self.lastFile = obj
    self.writable = false
    self.emit('end')
  }

  parser.onerror = function(err) {
    self.writable = false
    self.emit('error', new ParseError(err.error))
  }
}
inherits(Parser, Stream)
exports.Parser = Parser

Parser.prototype.write = function(chunk) {
  if (!this.writable) return
  this.parser.write(chunk.toString())
  return !this.paused
}
Parser.prototype.end = function(chunk) {
  if (!this.writable) return
  if (chunk) this.write(chunk)
  this.parser.end()
}
Parser.prototype.pause = function() {
  this.paused = true
  this.request.pause()
}
Parser.prototype.resume = function() {
  this.paused = false
  this.emit('drain')
  this.request.resume()
}
Parser.prototype.destroy = function() {
  this.writable = false
  this.request.destroy()
  this.request.abort()
}
