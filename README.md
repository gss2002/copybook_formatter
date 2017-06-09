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


#### Create Hive Table Layout:

	./copybook_formatter -convert tsv -gen_hive_only -appname FCUST -copybook_layout ./RCUSTDAT.cpy.txt -copybook_filetype MFVB -input ./fcust_src -output ./fcust_tran145 -include_record TRANSACTION-NBR=1:TRANSACTION-NBR=4:TRANSACTION-NBR=5


###### Verify Output
	hadoop fs -cat ./fcust_tran145/* | more
	
