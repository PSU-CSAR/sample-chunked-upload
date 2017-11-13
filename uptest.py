from __future__ import print_function

import hashlib
import requests
import os
import argparse
import uuid


CHUNKSIZE = 2**22  # 4 MB
DEFAULT_HOST = "ebagis.geog.pdx.edu"
AUTHURL = 'api/rest/token/'
UPLOADURL = 'api/rest/aois/'


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Upload a zipped AOI to ebagis.',
        fromfile_prefix_chars='@'
    )

    parser.add_argument('-u', '--username', type=str,
                        help='valid ebagis username')
    parser.add_argument('-p', '--password', type=str,
                        help='ebagis user password')
    parser.add_argument('-f', '--upfile', required=True, type=str,
                        help='zipped AOI directory')
    parser.add_argument('-n', '--filename', type=str, default=None,
                        help='name of the aoi; default is name of zip file')
    parser.add_argument(
        '-C',
        '--chunksize',
        type=int,
        default=CHUNKSIZE,
        help='size of upload chunks; default is {}'.format(CHUNKSIZE)
    )
    parser.add_argument(
        '--no-chunks',
        action='store_true',
        help='upload file in single HTTP POST request; default false'
    )
    parser.add_argument(
        '--no-https',
        action='store_false',
        dest='use_https',
        help='don\'t use HTTPS but insecure HTTP; default false '
             '(use only if you already know you need it)',
    )
    parser.add_argument('-c', '--comment', type=str,
                        help='a comment to add to the AOI')
    parser.add_argument('--parent-aoi', type=uuid.UUID,
                        help='the uuid of the parent AOI record in the DB')
    parser.add_argument('-r', '--host', type=str, default=DEFAULT_HOST,
                        help='the IP or domain name of the ebagis host')
    parser.add_argument('-P', '--port', type=int, default=None,
                        help='the port for ebagis; default is 443, '
                             'or 80 if no HTTPS')

    # parse the argvs pass in into args
    args = parser.parse_args(argv)

    return args


def generate_file_md5(filepath, blocksize=2**8):
    """find the md5 hash of a file"""
    m = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


class FileWrapper(object):
    """Wrapper to convert file-like objects to iterables,
    based on the FileWrapper class from wsgiref, but modified to
    take a start and end point to allow serving byte ranges in
    addition to whole files. Also fixes __getitem__ method to
    actually work as a getitem function should, so API is not
    exactly the same as the wsgiref implementation."""

    def __init__(self, filelike, blksize=8192, start=0, end=None):
        self.filelike = filelike
        self.blocksize = blksize
        self.start = start
        self.filelike.seek(start)
        self.end = end

        # get methods off filelike
        self.tell = filelike.tell
        self.read = filelike.read
        if hasattr(filelike, ' close'):
            self.close = filelike.close

    def __getitem__(self, key):
        return self._read(key=key)

    def __iter__(self):
        return self

    def _read(self, key=None):
        blocksize = self.blocksize
        current_position = self.tell()

        if key:
            self.seek(blocksize * key)

        if self.end and self.tell() + self.blocksize > self.end:
            blocksize = self.end - self.tell()

        data = self.read(blocksize)

        if key:
            self.seek(current_position)

        if data:
            return data

        raise IndexError

    def seek(self, position):
        position += self.start
        self.filelike.seek(position)

    def next(self):
        try:
            return self._read()
        except IndexError:
            raise StopIteration


class UploadTester(object):
    def __init__(self,
                 ebagis_host,
                 upload_file,
                 username=None,
                 password=None,
                 filename=None,
                 comment='',
                 chunksize=CHUNKSIZE,
                 parent_aoi=None,
                 use_https=True,
                 ebagis_port=None):
        self.username = username
        self.password = password

        self.login = True
        if username is None and password is None:
            self.login = False

        self._file = open(upload_file, 'rb')
        self.filename = filename if filename is not None else \
            os.path.basename(upload_file)
        self.comment = comment
        self.parent_aoi = None
        self.https = use_https
        self.host = ebagis_host

        self.port = ebagis_port

        self.md5 = generate_file_md5(upload_file)
        self.file_length = os.path.getsize(upload_file)

        self.file = FileWrapper(self._file, blksize=chunksize)

        self._base_url = 'https://' if self.https else 'http://'
        self._base_url += self.host
        self._base_url += ":{}/".format(self.port) if self.port else '/'

        self._token = None
        self._url = None
        self._header = None

    def __del__(self, *args, **kwargs):
        self._file.close()

    @property
    def token(self, refresh=False):
        if refresh or not self._token:
            authparams = {'username': self.username, 'password': self.password}
            authresp = requests.post(self._base_url + AUTHURL, data=authparams)
            try:
                self._token = authresp.json()['token']
            except KeyError:
                raise ValueError(
                    'ebagis username and/or password seems to be incorrect'
                )
        return self._token

    @property
    def header(self, refresh=False):
        if refresh or not self._header:
            header = {}
            if self.login:
                header['Authorization'] = 'Token {}'.format(self.token)
            self._header = header
        return self._header

    @property
    def params(self):
        return {
            'filename': self.filename,
            'comment': self.comment,
            'parent_object_id': self.parent_aoi,
        }

    @property
    def url(self):
        if not self._url:
            return self._base_url + UPLOADURL
        return self._url

    def upload(self, chunk_upload=True):
        if chunk_upload:
            while True:
                try:
                    self.put_chunk().raise_for_status()
                except StopIteration:
                    self.post_complete().raise_for_status()
                    break
        else:
            self.post_upload().raise_for_status()

    def put_chunk(self):
        # add a Content-Range parameter to the header
        # format of this is beginning of content, end of content,
        # and total size of content, in bytes
        header = self.header
        current_position = self.file.tell()
        chunkend = current_position + self.file.blocksize if \
            current_position + self.file.blocksize < self.file_length else \
            self.file_length
        header['Content-Range'] = 'bytes {}-{}/{}'.format(current_position,
                                                          chunkend,
                                                          self.file_length)
        data = self.file.next()
        resp = requests.put(self.url, headers=header, data=self.params,
                            files={'file': data})
        if not self._url and resp.status_code == requests.codes.ok:
            self._url = resp.json()['url']
        return resp

    def post_complete(self):
        params = self.params
        params['md5'] = self.md5
        return requests.post(self.url, headers=self.header, data=params)

    def post_upload(self):
        params = self.params
        params['md5'] = self.md5
        resp = requests.post(self.url, headers=self.header, data=params,
                             files={'file': self.file})
        return resp

    def reset(self, new_name=None):
        self._url = None
        if new_name:
            self.filename = new_name
        self.file.seek(0)


def main(argv=None):
    args = parse_args(argv)
    uploader = UploadTester(
        args.host,
        args.upfile,
        username=args.username,
        password=args.password,
        filename=args.filename,
        comment=args.comment,
        chunksize=args.chunksize,
        parent_aoi=args.parent_aoi,
        use_https=args.use_https,
        ebagis_port=args.port,
    )
    uploader.upload(chunk_upload=args.no_chunks)
    print('Upload Completed Successfully: {}'.format(uploader.url))


if __name__ == '__main__':
    main()
