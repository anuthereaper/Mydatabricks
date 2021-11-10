# Databricks notebook source
pip install pgpy

# COMMAND ----------

private_key = '/dbfs/mnt/anuadlstest/private.asc'
public_key = '/dbfs/mnt/anuadlstest/public.asc'
input_file = '/dbfs/mnt/anuadlstest/test1.csv'
pgp_file = '/dbfs/mnt/anuadlstest/test1.csv.pgp'
decrypted_file = '/dbfs/mnt/anuadlstest/testdecrypt.csv'

# COMMAND ----------

import pgpy
from pgpy.constants import PubKeyAlgorithm, KeyFlags, HashAlgorithm, SymmetricKeyAlgorithm, CompressionAlgorithm
from timeit import default_timer as timer
import base64
 
def generate_keys():
    # we can start by generating a primary key. For this example, we'll use RSA, but it could be DSA or ECDSA as well
    key = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)
    # we now have some key material, but our new key doesn't have a user ID yet, and therefore is not yet usable!
    uid = pgpy.PGPUID.new('testname', comment='PGP Assignment', email='testname@abc.com')
    # now we must add the new user id to the key. We'll need to specify all of our preferences at this point
    # because PGPy doesn't have any built-in key preference defaults at this time
    # this example is similar to GnuPG 2.1.x defaults, with no expiration or preferred keyserver
    key.add_uid(uid, usage={KeyFlags.Sign, KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage},
                hashes=[HashAlgorithm.SHA256, HashAlgorithm.SHA384, HashAlgorithm.SHA512, HashAlgorithm.SHA224],
                ciphers=[SymmetricKeyAlgorithm.AES256, SymmetricKeyAlgorithm.AES192, SymmetricKeyAlgorithm.AES128],
                compression=[CompressionAlgorithm.ZLIB, CompressionAlgorithm.BZ2, CompressionAlgorithm.ZIP, CompressionAlgorithm.Uncompressed])
    # assuming we already have a primary key, we can generate a new key and add it as a subkey thusly:
    subkey = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)
    # preferences that are specific to the subkey can be chosen here
    # any preference(s) needed for actions by this subkey that not specified here
    # will seamlessly "inherit" from those specified on the selected User ID
    key.add_subkey(subkey, usage={KeyFlags.EncryptCommunications})
    # ASCII armored
    with open(private_key, "w") as csv_file:
        csv_file.write(str(key))
    with open(public_key, "w") as csv_file:
        csv_file.write(str(key.pubkey))
    string_bytes = str(key).encode("ascii")
    privateb64 = base64.b64encode(string_bytes)
    print("Base64 encoded private key : " + str(privateb64))

generate_keys()

# COMMAND ----------

#Encrypting a file using public key
import pgpy
from pgpy.constants import PubKeyAlgorithm, KeyFlags, HashAlgorithm, SymmetricKeyAlgorithm, CompressionAlgorithm
from timeit import default_timer as timer
import base64 
import io

# Cutpaste the public key into the below to allow for encryption.
KEY_PUB = '''-----BEGIN PGP PUBLIC KEY BLOCK-----
 
xsFNBGELZfUBEAC3/zw3cj+8ZOQDrr8DdynSty1PLmGn6figuiGo5+9XKlrpzgnn
oxFo/9tuqr07MNzXL8g5xPLmWvStulZD5t8jrkKBM8Suscs2698T5YXkYfIQ4WV+
.
.
.
.
.
.
c5UGiFTCNmbZjo49VOvDWe5ykd3cGGVZj+GQD9MCuZi2Kt031hNO8BSxifz4ApNK
UO7g43emSDQF2GuqasYrUV6NiSx1J5/QYcd34xDTrrkcmR2tNSMJiwPR
=5UEt
-----END PGP PUBLIC KEY BLOCK-----'''.lstrip()  
 
pub_key = pgpy.PGPKey()
pub_key.parse(KEY_PUB)
pass
# ------------------
with io.open(input_file, "r",newline='') as csv_file:
    input_data = csv_file.read()                   # The io and newline retains the CRLF
    
t0 = timer()
#PGP Encryption start
msg = pgpy.PGPMessage.new(input_data)
###### this returns a new PGPMessage that contains an encrypted form of the unencrypted message
encrypted_message = pub_key.encrypt(msg)
pgpstr = str(encrypted_message)
with open(pgp_file, "w") as text_file:
    text_file.write(pgpstr)
print("Encryption Complete :" + str(timer()-t0))

# COMMAND ----------

#Decrypting a file using private key
import pgpy
from pgpy.constants import PubKeyAlgorithm, KeyFlags, HashAlgorithm, SymmetricKeyAlgorithm, CompressionAlgorithm
from timeit import default_timer as timer
import base64 
import io
 
def get_private_key():
    pk_base64 = dbutils.secrets.get(scope = "PGP", key = "pkbase64")
    pk_string = base64.b64decode(pk_base64)
    pk_string = pk_string.decode("ascii")
#    print("Base64 decoded private key :" + str(pk_string).lstrip())
    return str(pk_string)
 
private_key = get_private_key()
KEY_PRIV = private_key.lstrip()
 
priv_key = pgpy.PGPKey()
priv_key.parse(KEY_PRIV)
pass
 
#PGP Deryption start
t0 = timer()
message_from_file = pgpy.PGPMessage.from_file(pgp_file)
raw_message = priv_key.decrypt(message_from_file).message
with open(decrypted_file, "w") as csv_file:
    csv_file.write(raw_message.decode())
print("Decryption Complete :" + str(timer()-t0))

# COMMAND ----------

# This compares the original file with the decrypted file.
 
import sys
import hashlib
 
def hashfile(file):
    # A arbitrary (but fixed) buffer size (change accordingly)
    # 65536 = 65536 bytes = 64 kilobytes
    BUF_SIZE = 65536
    # Initializing the sha256() method
    sha256 = hashlib.sha256()
    # Opening the file provided as
    # the first commandline argument
    with open(file, 'rb') as f:
        while True:
            # reading data = BUF_SIZE from the file and saving it in a
            # variable
            data = f.read(BUF_SIZE)
            # True if eof = 1
            if not data:
                break
            # Passing that data to that sh256 hash function (updating the function with that data)
            sha256.update(data)
      
    # sha256.hexdigest() hashes all the input
    # data passed to the sha256() via sha256.update()
    # Acts as a finalize method, after which
    # all the input data gets hashed hexdigest()
    # hashes the data, and returns the output
    # in hexadecimal format
    return sha256.hexdigest()
 
# Calling hashfile() function to obtain hashes of the files, and saving the result in a variable
Initial_hash = hashfile(input_file)
Decrypted_hash = hashfile(decrypted_file)
  
# Doing primitive string comparison to
# check whether the two hashes match or not
if Initial_hash == Decrypted_hash:
    print("Both files are same")
    print(f"Hash: {Initial_hash}")
else:
    print("Files are different!")
    print(f"Hash of File 1: {Initial_hash}")
    print(f"Hash of File 2: {Decrypted_hash}")
