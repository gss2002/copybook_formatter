# Copybook Formatter

### To Build Copybook Formatter:
* Build tar/gz:
	mvn package -Ptar

* Build rpm:
	mvn package -Prpm


## Copybook Formatter Samples
##### Copy Sample Data into HDFS Only need to be done once:
	hadoop fs -copyFromLocal $HOME/copybook_formatter/samples/fcust_data ./fcust_src
	

##### Remove Output from Copybook Formatter jobs for samples if run before
	hadoop fs -rmr ./fcust_tran145

##### Run the job to convert from EBCDIC Copybook to TSV
#### Convert Copybook

	./copybook_formatter -convert tsv -appname FCUST -copybook_layout ./RCUSTDAT.cpy.txt -copybook_filetype MFVB -input ./fcust_src -output ./fcust_tran145 -include_record TRANSACTION-NBR=1:TRANSACTION-NBR=4:TRANSACTION-NBR=5

#### Split/Sort Copybook Options
	./copybook_formatter -sort -appname FCUST -sort_header_layout ./fcust_header.txt -sort_header_length 49 -sort_split_layout ./fcust_split.txt -copybook_filetype MFVB -input ./fcust_src -output ./fcust_split_sort_out -sort_split_length_name RECORD-LENGTH -sort_split_skip_value bf -sort_skip_record_name KEY-INDEX -sort_skip_record_value bf -traceall -sort_split_length_offset 3

#### Create Hive Table Layout:

	./copybook_formatter -convert tsv -gen_hive_only -appname FCUST -copybook_layout ./RCUSTDAT.cpy.txt -copybook_filetype MFVB -input ./fcust_src -output ./fcust_tran145 -include_record TRANSACTION-NBR=1:TRANSACTION-NBR=4:TRANSACTION-NBR=5


###### Verify Output
	hadoop fs -cat ./fcust_tran145/* | more
	
