# Databricks notebook source
# https://pycryptodome.readthedocs.io/en/latest/src/examples.html
#generate-an-rsa-key 
pip install PyCryptodome

# COMMAND ----------

#Encryption
from Crypto.Cipher import AES
import hashlib

input_file = '/dbfs/mnt/anuadlstest/test2.csv'
encrypted_file = '/dbfs/mnt/anuadlstest/test.csv.enc'
decrypted_file = '/dbfs/mnt/anuadlstest/test_aes_decrypted.csv'
    
password = "xxxxxxxxxxxxxxxxxxxxxxxx"
#key = hashlib.sha256(password).digest()
key = bytes(password, 'utf-8')
mode = AES.MODE_CBC
IV = bytes('This is an IV456', 'utf-8')
 
def pad_message(message):
    while len(message) % 16!= 0:
        message += ' '
    return message
 
cipher = AES.new(key, mode, IV)
with open(input_file, "r",newline='') as csv_file:
    message = csv_file.read()
padded_message = pad_message(message)    
 
encrypted_text = cipher.encrypt(bytes(padded_message, 'utf-8'))
with open(encrypted_file, "wb") as text_file:
    text_file.write(encrypted_text)

print(encrypted_text)

# COMMAND ----------

#Decryption
from Crypto.Cipher import AES
import hashlib
 
password = password
#key = hashlib.sha256(password).digest()
key = bytes(password, 'utf-8')
mode = AES.MODE_CBC
IV = bytes('This is an IV456', 'utf-8')
 
def pad_message(message):
    while len(message) % 16!= 0:
        message += ' '
    return message
 
cipher = AES.new(key, mode, IV)
with open(encrypted_file, "rb") as csv_file:
    message = csv_file.read()
print(message)
#encrypted_text = bytes(message,'utf-8')
#encrypted_text = pad_message(message)   
decrypted_text = cipher.decrypt(message)
decrypted_text = str(decrypted_text.rstrip().decode("utf-8"))   # strip of the suffix padding
print(decrypted_text)
with open(decrypted_file, "w") as text_file:
    text_file.write(decrypted_text)

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
