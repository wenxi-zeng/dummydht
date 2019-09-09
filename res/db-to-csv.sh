filename="$(ls /root/dummydhtdb/*.db)";
for curr_filename in ${filename[@]}
do
  db=$curr_filename
  t=($(sqlite3 $db ".tables"))
  for i in "${t[@]}"
    do
      sqlite3 -csv $db "select * from $i;" > $curr_filename-$i.csv
      echo "$curr_filename-$i.csv generated"
    done
done

