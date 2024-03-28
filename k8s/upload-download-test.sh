#!/bin/bash

port=445

# generate and upload some files
for i in $(seq 1 5); do
  fname="file-${i}"
  dd if=/dev/urandom of=${fname} bs=8M count=1
  smbclient -p ${port} -U nexoedge //127.0.0.1/nexoedge nexoedge -c "put ${fname}"
done

# download the files
for i in $(seq 1 5); do
  fname="file-${i}"
  smbclient -p ${port} -U nexoedge //127.0.0.1/nexoedge nexoedge -c "get ${fname} download-${i}"
done

# check if each downloaded file is the same as the corresponding source file
for i in $(seq 1 5); do
  fname="file-${i}"
  cmp ${fname} download-${i}
  if [ $? -eq 0 ]; then
    echo "Check on file ${i} passes."
  else
    echo "Check on file ${i} failes."
  fi
  rm ${fname} download-${i}
done
