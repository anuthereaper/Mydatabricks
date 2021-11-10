# Databricks notebook source
# https://pycryptodome.readthedocs.io/en/latest/src/examples.html
#generate-an-rsa-key 
pip install PyCryptodome

# COMMAND ----------

#Generate public key and private key
#The following code generates public key stored in receiver.pem and private key stored in private.pem. 
from Crypto.PublicKey import RSA
 
private_key = '/dbfs/mnt/anuadlstest/private.pem'
public_key '/dbfs/mnt/anuadlstest/public.pem'
input_file = '/dbfs/mnt/anuadlstest/test1.csv'
encrypted_file = '/dbfs/mnt/anuadlstest/test.csv.rsaencrypted'
decrypted_file = '/dbfs/mnt/anuadlstest/test_rsa_decrypted.csv'

key = RSA.generate(4096)
private_key = key.export_key()
with open(private_key, "wb") as csv_file:
      csv_file.write(private_key)

public_key = key.publickey().export_key()
with open(public_key, "wb") as csv_file:
      csv_file.write(public_key)

# COMMAND ----------

#RSA Encryption
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES, PKCS1_OAEP
 
file_out = open(encrypted_file, "wb")
 
with open(input_file, "r",newline='') as csv_file:
    data = csv_file.read() 
    
with open(public_key, "r",newline='') as csv_file:
    public_key = csv_file.read()    

recipient_key = RSA.import_key(public_key)
session_key = get_random_bytes(16)
 
# Encrypt the session key with the public RSA key
cipher_rsa = PKCS1_OAEP.new(recipient_key)
enc_session_key = cipher_rsa.encrypt(session_key)
 
# Encrypt the data with the AES session key
cipher_aes = AES.new(session_key, AES.MODE_EAX)
ciphertext, tag = cipher_aes.encrypt_and_digest(bytes(data, 'utf-8'))
[ file_out.write(x) for x in (enc_session_key, cipher_aes.nonce, tag, ciphertext) ]
file_out.close()

# COMMAND ----------

#RSA Decryption
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
 
file_in = open(encrypted_file, "rb")
 
private_key = RSA.import_key(open(private_key).read())
 
enc_session_key, nonce, tag, ciphertext = \
   [ file_in.read(x) for x in (private_key.size_in_bytes(), 16, 16, -1) ]
 
# Decrypt the session key with the private RSA key
cipher_rsa = PKCS1_OAEP.new(private_key)
session_key = cipher_rsa.decrypt(enc_session_key)
 
# Decrypt the data with the AES session key
cipher_aes = AES.new(session_key, AES.MODE_EAX, nonce)
data = cipher_aes.decrypt_and_verify(ciphertext, tag)
with open(decrypted_file, "w") as csv_file:
      csv_file.write(data.decode("utf-8"))

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
