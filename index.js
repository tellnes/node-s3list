var PassThrough = require('stream').PassThrough
  , Transform = require('stream').Transform
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
    stream.push(null)
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
    stream.parser = parser

    req.on('response', function(res) {
      res.setEncoding('utf8')
      res.on('error', onerror)
      res.pipe(parser)
    })

    req.on('error', onerror)
    parser.on('error', onerror)

    parser.pipe(stream, { end: false })
    parser.on('end', onend)
  }

  execute()

  return stream
}

function S3ListStream() {
  PassThrough.call(this, { objectMode: true })
  this.commonPrefixes = []
}
inherits(S3ListStream, PassThrough)
exports.S3ListStream = S3ListStream

S3ListStream.prototype.destroy = function() {
  if (!this.parser) return
  this.parser.destroy()
}




function Parser(request) {
  Transform.call(this, { decodeStrings: false, readableObjectMode: true })

  var parser = new SAXParser(true)
    , meta = {}
    , state = 'start'
    , self = this
    , obj
    , key


  this.request = request

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
        self.push(obj)
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
    case 'file:StorageClass':
      obj[key] = text
      break

    case 'file:ETag':
      obj[key] = text.slice(1, -1)
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
    self.push(null)
  }

  parser.onerror = function(err) {
    self.emit('error', new ParseError(err.error))
  }
}
inherits(Parser, Transform)
exports.Parser = Parser

Parser.prototype._transform = function(chunk, enc, cb) {
  this.parser.write(chunk.toString())
  cb()
}

Parser.prototype._flush = function() {
  this.parser.end()
}

Parser.prototype.destroy = function() {
  this.request.destroy()
  this.request.abort()
}
