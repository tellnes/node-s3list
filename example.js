var s3list = require('./')
  , inspect = require('eyes').inspector()

var list = s3list({ key: process.env.AWS_KEY
                  , secret: process.env.AWS_SECRET
                  , bucket: process.argv[2]
                  })

var counter = 0

list.on('data', function(file) {
  inspect(file, 'File')
  counter++
})

// Every request to s3
list.on('progress', function() {
  inspect(counter, 'progress')
})

// Can be everything from an s3 error to an socket error
list.on('error', function(err) {
  inspect(err)
})
