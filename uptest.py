import hashlib
import requests
import os


USERNAME =
PASSWORD =

upfile = r"D:\projects\ebagis\DatabaseInterfaceTesting\AOIs\teton_aoi_111114.zip"
chunksize = 2**22  # 4 MB
rooturl = "http://localhost:8000/api/rest/"
auth_url = 'api-token-auth/'
upload_url = 'aoiuploads/'
authparams = {'username': USERNAME, 'password': PASSWORD}


def generate_file_sha1(filepath, blocksize=2**8):
    """find the md5 hash of a file"""
    m = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


def chunk_file(filepath, blocksize=2**12):
    """break a file in to chunks by the specified
    blocksize and yield each chunk in order"""
    with open(filepath, 'rb') as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            yield buf


# POST to get token for username/password
authresp = requests.post(rooturl + auth_url, data=authparams)
print authresp

# assemble header with Authorization via token
header = {"Authorization": "Token {}".format(authresp.json()['token'])}
print header

# find the md5 of the upfile and get total size
md5 = generate_file_sha1(upfile)
filebytes = os.path.getsize(upfile)

print md5
print filebytes

# create a dict of parameters to pass for the upload
# note: md5 is only needed for final POST, but won't
# break PUTs
params = {'filename': os.path.basename(upfile), 'md5': md5}

# simply GET user's uploads to verify token works
getresp = requests.get(rooturl + upload_url, headers=header)
# print the response code: 200 is good
print getresp

# get a generator of the chunks
# translation: chunks will "give" the chunks of the
# file based on the blocksize, in order
chunks = chunk_file(upfile, blocksize=chunksize)

# get first chunk and add it to files dict
chunk = chunks.next()
files = {'file': chunk}

# add a Content-Range parameter to the header
# format of this is beginning of content, end of content,
# and total size of content, in bytes
header['Content-Range'] = 'bytes {}-{}/{}'.format(0,
                                                  chunksize,
                                                  filebytes)

# PUT the first chunk to the upload url
postresp = requests.put(rooturl + upload_url, headers=header, data=params,
                        files=files)

print postresp
# get response as dictionary
respdict = postresp.json()
print respdict

# iterate through remaining chunks
for index, chunk in enumerate(chunks, 1):
    # get offset from previous response
    # TODO: add check to make sure offset is as expected
    offset = respdict['offset']
    # calculate new chunk end byte
    # if calculated value is greater than filesize, just use file size
    chunkend = chunksize*(index+1) if chunksize*(index+1) < filebytes else filebytes
    # set Content-Range with new values
    header['Content-Range'] = 'bytes {}-{}/{}'.format(offset,
                                                      chunkend,
                                                      filebytes)
    # files gets new chunk
    files = {'file': chunk}
    # PUT current chunk to upload url from response
    postresp = requests.put(respdict['url'], headers=header,
                        files=files)

    print postresp
    respdict = postresp.json()
    print respdict

# POST to upload url with md5 to complete upload
postresp = requests.post(respdict['url'], headers=header, data=params)
print postresp
print postresp.json()
