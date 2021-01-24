const multer  = require('multer');
const multers3= require('multer-s3');
const aws     = require('aws-sdk');
const config  = require('../config.json');

aws.config.update({
  secretAccessKey: config.secretAccessKey,
  accessKeyId: config.accessKeyId,
  region: 'us-east-2' //ohio
});

const s3 = new aws.S3();
const fileFilter = (req, file, cb) => {
  //console.log('mimetype:', file.mimetype);
  if (file.mimetype === 'image/jpeg' || file.mimetype === 'image/png') {
    cb(null, true);
  } else {
    cb(new Error('Invalid file type, only JPEG and PNG is allowed!'), false);
  }
};

const upload  = multer({
  storage: multers3({
    s3: s3,
    acl: 'public-read',
    bucket: 'devtrackeroverlord',
    key: function (req, file, cb) {
      //console.log(file);
      var newFileName = Date.now() + "-" + file.originalname;
      var fullPath = 'images/'+ newFileName;
      cb(null, fullPath); //use Date.now() for unique file keys
    }
  })
});

module.exports = upload;