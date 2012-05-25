# S3list

S3list makes it easy to stream files in an S3 bucket.


## Example

```js
var s3list = require('s3list')

var list = s3list({ key: 'AWS Access key'
                  , secret: 'AWS Access secret'
                  , bucket: 's3 bucket'
                  })

list.on('data', function(file) {
  console.log(file)
})
```


## Install

    npm install s3list


## Licence

MIT
