var _ = require('lodash');
var ALY = require('aliyun-sdk');
var Busboy = require('busboy');
var moment = require('moment');
var stream = require('stream');

function AliOssService(options) {
    var self = this;

    if (!(self instanceof AliOssService)) {
        return new AliOssService(options);
    }

    self.options = options;

    self.oss = new ALY.OSS({
        accessKeyId: self.options.accessKeyId,
        secretAccessKey: self.options.accessKeySecret,
        endpoint: 'http://' + self.options.region + (self.options.internal ? '-internal' : '') + '.aliyuncs.com',
        apiVersion: '2013-10-15'
    });

    self.ossStream = require('aliyun-oss-upload-stream')(self.oss);
}

var errorHandler = function (err) {
    var result = new Error((err && err.message) || (err && err.name) || 'Unknown Error');
    result.statusCode = (err && err.statusCode) || 500;
    return result;
};

/**
 * List all storage service containers.
 * @callback {Function} callback Callback function
 * @param {Object|String} err Error string or object
 * @param {Object[]} containers An array of container metadata objects
 */
AliOssService.prototype.getContainers = function (cb) {
    var self = this;

    if (_.isString(self.options.bucket)) {
        self.oss.listObjects({
            Bucket: self.options.bucket,
            MaxKeys: '1000',
            Delimiter: '/'
        }, function (err, result) {
            if (err || !result) {
                cb(errorHandler(err));
            } else {
                cb(null, _.map(result.CommonPrefixes, function (x) {
                    return {
                        name: _.trim(x.Prefix, '/')
                    };
                }));
            }
        });
    } else {
        self.oss.listBuckets(function (err, result) {
            if (err || !result) {
                cb(errorHandler(err));
            } else {
                cb(null, _.map(result.Buckets, function (x) {
                    return {
                        name: x.Name
                    };
                }));
            }
        });
    }
};

/**
 * Create a new storage service container.
 *
 * @options {Object} options Options to create a container. Option properties depend on the provider.
 * @prop {String} name Container name
 * @callback {Function} cb Callback function
 * @param {Object|String} err Error string or object
 * @param {Object} container Container metadata object
 */

AliOssService.prototype.createContainer = function (options, cb) {
    var self = this;
    var container = options && options.name;

    if (_.isString(self.options.bucket)) {
        self.oss.putObject({
            Bucket: self.options.bucket,
            Key: container + '/'
        }, function (err, content) {
            if (err || !content) {
                cb(errorHandler(err));
            } else {
                cb(null, {
                    name: container
                });
            }
        });
    } else {
        self.oss.createBucket({
            Bucket: container
        }, function (err, content) {
            if (err || !content) {
                cb(errorHandler(err));
            } else {
                cb(null, {
                    name: container
                });
            }
        });
    }
};

/**
 * Destroy an existing storage service container.
 * @param {String} container Container name.
 * @callback {Function} callback Callback function.
 * @param {Object|String} err Error string or object
 */
AliOssService.prototype.destroyContainer = function (container, cb) {
    var self = this;

    if (_.isString(self.options.bucket)) {
        var options = {};
        options['Bucket'] = self.options.bucket;
        options['MaxKeys'] = 1000;
        options['Prefix'] = container + '/';
        options['Delimiter'] = '/';

        self.oss.listObjects(options, function (err, result) {
            if (err || !result) {
                cb(errorHandler(err));
            } else {
                self.oss.deleteObjects({
                    Bucket: self.options.bucket,
                    Delete: {
                        Objects: _.map(result.Contents, function (content) {
                            return {
                                Key: content.Key
                            };
                        }),
                        Quiet: true
                    }
                }, function (err) {
                    if (err)
                        cb(errorHandler(err));
                    else
                        cb();
                });
            }
        });
    } else {
        self.oss.deleteBucket({
            Bucket: container
        }, function (err) {
            if (err)
                cb(errorHandler(err));
            else
                cb();
        });
    }
};

/**
 * Look up a container metadata object by name.
 * @param {String} container Container name.
 * @callback {Function} callback Callback function.
 * @param {Object|String} err Error string or object
 * @param {Object} container Container metadata object
 */
AliOssService.prototype.getContainer = function (container, cb) {
    var self = this;

    if (_.isString(self.options.bucket)) {
        self.oss.headObject({
            Bucket: self.options.bucket,
            Key: container + '/'
        }, function (err, content) {
            if (err || !content) {
                cb(errorHandler(err));
            } else {
                try {
                    cb(null, {
                        name: container
                    });
                } catch (err) {
                    cb(err);
                }
            }
        });
    } else {
        self.oss.getBucketLocation({
            Bucket: container
        }, function (err, data) {
            if (err || !data) {
                cb(errorHandler(err));
            } else {
                cb(null, {
                    name: container
                });
            }
        });
    }
};

/**
 * Get the stream for uploading
 * @param {String} container Container name
 * @param {String} file  File name
 * @options {Object} [options] Options for uploading
 * @callback callback Callback function
 * @param {String|Object} err Error string or object
 * @returns {Stream} Stream for uploading
 */
AliOssService.prototype.uploadStream = function (container, file, options) {
    var self = this;

    if (_.isFunction(options)) {
        options = {};
    }
    options = options || {};

    var key;
    if (_.isString(self.options.bucket)) {
        key = container + '/' + file;
    } else {
        key = file;
    }

    return self.ossStream.upload({
        Bucket: self.options.bucket || container,
        Key: key
    });
};

/**
 * Get the stream for downloading.
 * @param {String} container Container name.
 * @param {String} file File name.
 * @options {Object} options Options for downloading
 * @callback {Function} callback Callback function
 * @param {String|Object} err Error string or object
 * @returns {Stream} Stream for downloading
 */
AliOssService.prototype.downloadStream = function (container, file, options) {
    var self = this;

    if (_.isFunction(options)) {
        options = {};
    }
    options = options || {};

    var bucket;
    var key;
    if (_.isString(self.options.bucket)) {
        bucket = self.options.bucket;
        key = container + '/' + file;
    } else {
        bucket = container;
        key = file;
    }

    self.oss.getObject({
        Bucket: bucket,
        Key: key
    }, function (err, content) {
        if (err || !content) {
            throw errorHandler(err);
        } else {
            var bufferStream = new stream.PassThrough();
            bufferStream.end(content.body);
            cb(null, bufferStream);
        }
    });
};

/**
 * List all files within the given container.
 * @param {String} container Container name.
 * @param {Object} [options] Options for download
 * @callback {Function} cb Callback function
 * @param {Object|String} err Error string or object
 * @param {Object[]} files An array of file metadata objects
 */
AliOssService.prototype.getFiles = function (container, options, cb) {
    var self = this;

    if (_.isFunction(options)) {
        cb = options;
        options = {};
    }
    options = options || {};

    if (_.isString(self.options.bucket)) {
        options['Bucket'] = self.options.bucket;
        options['MaxKeys'] = 1000;
        options['Prefix'] = container + '/';
        options['Delimiter'] = '/';
    } else {
        options['Bucket'] = container;
        options['MaxKeys'] = 1000;
        options['Delimiter'] = '/';
    }

    self.oss.listObjects(options, function (err, result) {
        if (err || !result) {
            cb(errorHandler(err));
        } else {
            cb(null, _.filter(_.map(result.Contents, function (content) {
                return {
                    name: result.Prefix ? content.Key.replace(result.Prefix, '') : content.Key,
                    lastModified: _.isDate(content.LastModified) ? content.LastModified : moment(content.LastModified).toDate(),
                    etag: _.trim(content.ETag, '"'),
                    size: content.Size
                };
            }), function (content) {
                return content.name !== '';
            }));
        }
    });
};

/**
 * Look up the metadata object for a file by name
 * @param {String} container Container name
 * @param {String} file File name
 * @callback {Function} cb Callback function
 * @param {Object|String} err Error string or object
 * @param {Object} file File metadata object
 */
AliOssService.prototype.getFile = function (container, file, cb) {
    var self = this;

    var bucket;
    var key;
    if (_.isString(self.options.bucket)) {
        bucket = self.options.bucket;
        key = container + '/' + file;
    } else {
        bucket = container;
        key = file;
    }
    self.oss.headObject({
        Bucket: bucket,
        Key: key
    }, function (err, content) {
        if (err || !content) {
            cb(errorHandler(err));
        } else {
            cb(null, {
                name: file,
                lastModified: _.isDate(content.LastModified) ? content.LastModified : moment(content.LastModified).toDate(),
                etag: _.trim(content.ETag, '"'),
                size: parseInt(content.ContentLength),
                metadata: content.Metadata
            });
        }
    });
};

/**
 * Remove an existing file
 * @param {String} container Container name
 * @param {String} file File name
 * @callback {Function} cb Callback function
 * @param {Object|String} err Error string or object
 */
AliOssService.prototype.removeFile = function (container, file, cb) {
    var self = this;

    var bucket;
    var key;
    if (_.isString(self.options.bucket)) {
        bucket = self.options.bucket;
        key = container + '/' + file;
    } else {
        bucket = container;
        key = file;
    }
    self.oss.deleteObject({
        Bucket: bucket,
        Key: key
    }, function (err) {
        if (err)
            cb(errorHandler(err));
        else
            cb();
    });
};

/**
 * Upload middleware for the HTTP request/response  <!-- Should self be documented? -->
 * @param {Request} req Request object
 * @param {Response} res Response object
 * @param {Object} [options] Options for upload
 * @param {Function} cb Callback function
 */
AliOssService.prototype.upload = function (req, res, options, cb) {
    var self = this;

    if (!cb && _.isFunction(options)) {
        cb = options;
        options = {};
    }

    var busboy = new Busboy({
        headers: req.headers
    });

    busboy.on('file', function (fieldname, file, filename, encoding, mimetype) {
        var bucket;
        var key;
        if (_.isString(self.options.bucket)) {
            bucket = self.options.bucket;
            key = (options.container || req.params.container) + '/' + filename;
        } else {
            bucket = options.container || req.params.container;
            key = file;
        }

        var upload = self.ossStream.upload({
            Bucket: self.options.bucket || container,
            Key: key
        });

        upload.on('uploaded', function (details) {
          cb(null, details);
        });

        file.pipe(upload);
    });

    req.pipe(busboy);
};

/**
 * Download middleware 
 * @param {String} container Container name
 * @param {String} file File name
 * @param {Request} req HTTP request
 * @param {Response} res HTTP response
 * @param {Function} cb Callback function
 */
AliOssService.prototype.download = function (container, file, req, res, cb) {
    var self = this;

    var bucket;
    var key;
    if (_.isString(self.options.bucket)) {
        bucket = self.options.bucket;
        key = container + '/' + file;
    } else {
        bucket = container;
        key = file;
    }

    self.oss.getObject({
        Bucket: bucket,
        Key: key
    }, function (err, content) {
        if (err || !content) {
            res.type('applicaiton/json');
            res.status((err && err.statusCode) || 500).send(errorHanlder(err));
        } else {
            res.set('Content-Disposition', 'attachment;filename=' + file);
            res.set('Content-Type', content.ContentType ? content.ContentType : 'application/octet-stream');
            res.send(content.Body);
        }
    });
};

AliOssService.modelName = 'storage';

AliOssService.prototype.getContainers.shared = true;
AliOssService.prototype.getContainers.accepts = [];
AliOssService.prototype.getContainers.returns = {
    arg: 'containers',
    type: 'array',
    root: true
};
AliOssService.prototype.getContainers.http = {
    verb: 'get',
    path: '/'
};

AliOssService.prototype.getContainer.shared = true;
AliOssService.prototype.getContainer.accepts = [
    {
        arg: 'container',
        type: 'string',
        required: true
    }
];
AliOssService.prototype.getContainer.returns = {
    arg: 'container',
    type: 'object',
    root: true
};
AliOssService.prototype.getContainer.http = {
    verb: 'get',
    path: '/:container'
};

AliOssService.prototype.createContainer.shared = true;
AliOssService.prototype.createContainer.accepts = [
    {
        arg: 'options',
        type: 'object',
        http: {
            source: 'body'
        }
    }
];
AliOssService.prototype.createContainer.returns = {
    arg: 'container',
    type: 'object',
    root: true
};
AliOssService.prototype.createContainer.http = {
    verb: 'post',
    path: '/'
};

AliOssService.prototype.destroyContainer.shared = true;
AliOssService.prototype.destroyContainer.accepts = [
    {
        arg: 'container',
        type: 'string',
        required: true
    }
];
AliOssService.prototype.destroyContainer.returns = {};
AliOssService.prototype.destroyContainer.http = {
    verb: 'delete',
    path: '/:container'
};

AliOssService.prototype.getFiles.shared = true;
AliOssService.prototype.getFiles.accepts = [
    {
        arg: 'container',
        type: 'string',
        required: true
    }
];
AliOssService.prototype.getFiles.returns = {
    arg: 'files',
    type: 'array',
    root: true
};
AliOssService.prototype.getFiles.http = {
    verb: 'get',
    path: '/:container/files'
};

AliOssService.prototype.getFile.shared = true;
AliOssService.prototype.getFile.accepts = [
    {
        arg: 'container',
        type: 'string',
        required: true
    },
    {
        arg: 'file',
        type: 'string',
        required: true
    }
];
AliOssService.prototype.getFile.returns = {
    arg: 'file',
    type: 'object',
    root: true
};
AliOssService.prototype.getFile.http = {
    verb: 'get',
    path: '/:container/files/:file'
};

AliOssService.prototype.removeFile.shared = true;
AliOssService.prototype.removeFile.accepts = [
    {
        arg: 'container',
        type: 'string',
        required: true
    },
    {
        arg: 'file',
        type: 'string',
        required: true
    }
];
AliOssService.prototype.removeFile.returns = {};
AliOssService.prototype.removeFile.http = {
    verb: 'delete',
    path: '/:container/files/:file'
};

AliOssService.prototype.upload.shared = true;
AliOssService.prototype.upload.accepts = [
    {
        arg: 'req',
        type: 'object',
        'http': {
            source: 'req'
        }
    },
    {
        arg: 'res',
        type: 'object',
        'http': {
            source: 'res'
        }
    }
];
AliOssService.prototype.upload.returns = {
    arg: 'result',
    type: 'object'
};
AliOssService.prototype.upload.http = {
    verb: 'post',
    path: '/:container/upload'
};

AliOssService.prototype.download.shared = true;
AliOssService.prototype.download.accepts = [
    {
        arg: 'container',
        type: 'string',
        http: {
            source: 'path'
        },
        required: true
    },
    {
        arg: 'file',
        type: 'string',
        http: {
            source: 'path'
        },
        required: true
    },
    {
        arg: 'req',
        type: 'object',
        'http': {
            source: 'req'
        }
    },
    {
        arg: 'res',
        type: 'object',
        'http': {
            source: 'res'
        }
    }
];
AliOssService.prototype.download.http = {
    verb: 'get',
    path: '/:container/download/:file'
};

module.exports = AliOssService;