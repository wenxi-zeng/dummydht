filename="$(ls /root/dummydhtdb/*.db)";
for curr_filename in ${filename[@]}
do
  db=$curr_filename
  sqlite3 $db "PRAGMA wal_checkpoint;"
done

