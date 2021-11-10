# Databricks notebook source
# MAGIC %sh
# MAGIC pip install pyzipper

# COMMAND ----------

import pyzipper
 
secret_password = b'xxxxxxx'
input_file = ''/dbfs/mnt/anuadlstest/test1/newfile1.zip'
output_file = '/dbfs/mnt/anuadlstest/test1/unzipped1.csv'

#Unzipping
with pyzipper.AESZipFile(input_file) as zf:
    zf.setpassword(secret_password)
    my_unzipped_data = zf.read('newfile.csv')
print("byte format")
print(my_unzipped_data)
print("string format")
print(my_unzipped_data.decode("utf-8")) 
with open(output_file, "w") as text_file:
    text_file.write(my_unzipped_data.decode("utf-8"))

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
Initial_hash = hashfile(input_file)           # Original file
Decrypted_hash = hashfile(output_file)       # Unzipped file
  
# Doing primitive string comparison to
# check whether the two hashes match or not
if Initial_hash == Decrypted_hash:
    print("Both files are same")
    print(f"Hash: {Initial_hash}")
else:
    print("Files are different!")
    print(f"Hash of File 1: {Initial_hash}")
    print(f"Hash of File 2: {Decrypted_hash}")
