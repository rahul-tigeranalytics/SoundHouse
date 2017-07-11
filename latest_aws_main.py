#!/usr/bin/env python
import sys
import glob
import MySQLdb
import datetime
import traceback
import logging
import calendar
from dateutil.parser import parse
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import lit,lower,upper
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, udf
from pyspark import SparkContext, SparkConf
import s3fs

class ETL_Processing(object):
    def __init__(self,sqlCtx,property_file_path,logger):
        """
        This __init__() method initialises the "self.*" variables,
        which will be in used across other methods of the ETL_Processing class
        :param sqlCtx:              Pyspark sql context
        :type sqlCtx:               pyspark.sql.context.SQLContext
        :param property_file_path:  Property file path
        :type property_file_path:   str
        :param logger:              Logger Object
        :return:                    Does not return anything from this method.
        :rtype:                     None
        """
        logger.info("Inside __init__() method ")
        self.logger=logger
        self.sqlCtx = sqlCtx
        self.url="jdbc:mysql://soundhousemasterdb.c4q9tpcfjner.us-east-2.rds.amazonaws.com:3306/SoundHouse?user=dbadmin&password=dbadmin123&autoReconnect=true&useSSL=false"
        self.properties={"driver":'com.mysql.jdbc.Driver'}
        fs = s3fs.S3FileSystem(anon=True)
        meta_file = open('/tmp/meta_file.py', 'w')
        with fs.open('soundhousellc/'+str(property_file_path.split('soundhousellc/')[1])) as f:
            meta_file.write(f.read())
            # print f.read()
            # import pdb;pdb.set_trace()
        meta_file.close()
        execfile('/tmp/meta_file.py')
        #fs.close()
        # print locals()['metadata_property']
        # import pdb;pdb.set_trace()
        # The property file should always have the dict variable name as "metadata_property"
        self.metadata_property = locals()['metadata_property']
        self.mysqldb_port = "soundhousemasterdb.c4q9tpcfjner.us-east-2.rds.amazonaws.com"
        self.mysqldb_user = "dbadmin"
        self.mysqldb_password = "dbadmin123"
        self.mysqldb_database_name = "SoundHouse"
        self.title_case_cols = ['Artist','Track','Album','Service_Provider','Service_Type','Territory']
        return
    def report_date_parsing(self,file_name):
        """
        This method parses the report date from the file name.
        All the source files need to follow the following naming conventions:
            Example File Name :  *_Apr_2016.csv
        Here we parse the last part of the file name "_Apr_2016" and arrive at "2016-04-30" as report date

        :param file_name:   This is the file name of source file
        :type file_name:    str
        :return:            Returns a tuple containing report date as both datetime and string object
        :rtype:              (datetime, str)
        """
        try:
            logger.info("Inside report_date_parsing() method ")
            print file_name
            file_split= file_name.split('.csv')[0].split('_')
            year = str(file_split[-1])
            qt_months= {'QT1': 'Mar', 'QT2': 'Jun', 'QT3': 'Sep', 'QT4': 'Dec'}
            # print qt_months
            month_split =file_split[-2]
            # print month_split
            if str(month_split).upper() in qt_months.keys():
                qt_key= str(month_split).upper()
                month = str(parse(file_split[-1] +" " +qt_months[qt_key]+" 01").month)  #qt_months[qt_key]
                day = str(calendar.monthrange(int(file_split[-1]),parse(file_split[-1] +" " +qt_months[qt_key]+ " 01").month)[1])
            else:
                month = str(parse(file_split[-1] +" " +file_split[-2]+" 01").month)
                day = str(calendar.monthrange(int(file_split[-1]),parse(file_split[-1] +" " +file_split[-2]+ " 01").month)[1])
            # print year, month, day
            report_date = parse(year+" "+ month+" "+day)
            report_date_string =str(report_date.date())
            #report_date = calendar.monthrange(file_split[-1],file_split[-2])
            return report_date, report_date_string
        except Exception,e:
            raise Exception("Error while executing report_date_parsing() :"+str(e))
    def deal_vendor_parsing(self,file_path):
        """
        This method parses the deal and vendor from the given source file path.
            Example source file path : /home/Source_files/Anthem Lights/SX-RO/
        Here we split and take the last 2 folder names as <Deal Name>/<Vendor Name>
        :param file_path:           File path of the source files
        :type file_path:            str
        :return:                    Does not return anything from this method.
        :rtype:                     None

        """
        try:
            logger.info("Inside deal_vendor_parsing() method ")
            file_path_split= file_path.split('/')
            #logger.info("FILE PATHHHHHHHHHHH SPLIT  : "+str(file_path_split))
            self.vendor_name = file_path_split[-2]
            self.deal_name = file_path_split[-3]
            logger.info("Deal Name : "+str(self.deal_name)+" Vendor Name : "+str(self.vendor_name))
            return
        except Exception,e:
            raise Exception("Error while executing deal_vendor_parsing() :"+str(e))
    def check_file_processed(self,file_name):
        """
        This checks if the file has already been processed by checking the File_Summary table.
        :param file_name:   File Name that is to be processed
        :type file_name :   str
        :return:            Returns "processed_flag"
                            If processed_flag >= 1 then file has been processed already
                            Else if processed_flag =0 then it means the file has not been processed
        :rtype:             int
        """
        try:
            logger.info("Inside check_file_processed() method ")
            file_summary_table = self.sqlCtx.read.jdbc(url=self.url, table="File_Summary", properties=self.properties) #sqlContext.read.jdbc
            file_summary_table.registerTempTable("file_summary")
            file_processed_result = self.sqlCtx.sql('SELECT * FROM file_summary WHERE file_name = "'+str(file_name)+'" AND deal_name = "'+str(self.deal_name)+'" AND vendor_name = "'+str(self.vendor_name)+'"')
            processed_flag = file_processed_result.count()
            logger.info("Finished check_file_processed() ")
            logger.info("Returning Processed flag = "+str(processed_flag))
            return processed_flag
        except Exception,e:
            raise Exception("Error while executing check_file_processed() :"+str(e))
    def metadata_management_versioning(self,df,report_date_string):
        """
        This is method is to track the versions of the files within the given Deal - Vendor combination.
        Here every time a new combination of headers are found for the given Deal - Vendor combination then,
        it inserts into the the Deal , Vendor and the headers as comma sepearated string into the Metadata_Management table.
        :param df:                  Data Frame being  processed
        :type df:                   pyspark.sql.dataframe.DataFrame
        :param report_date_string:  Report Date
        :type report_date_string:   str
        :return:                    Does not return anything from this method.
        :rtype:                     None
        """
        try:
            logger.info("Inside metadata_management_versioning() method ")
            # source_column_list = df.columns
            source_column_list = [str(x).strip().lower() for x in df.columns]
            source_column_string= ','.join(source_column_list)
            metadata_management_table = self.sqlCtx.read.jdbc(url=self.url, table="Metadata_Management", properties=self.properties) #sqlContext.read.jdbc
            metadata_management_table.registerTempTable("metadata_management")
            meta_versions = self.sqlCtx.sql("SELECT * FROM metadata_management WHERE deal_name='"+str(self.deal_name)+"' AND vendor_name= '"+str(self.vendor_name)+"' and file_headers='"+str(source_column_string)+"'")
            if not meta_versions.count():
                meta_insert_df= self.sqlCtx.createDataFrame([(self.deal_name,self.vendor_name,report_date_string,source_column_string,str(datetime.datetime.now()))], ['deal_name', 'vendor_name','report_date','file_headers','create_date'])
                meta_insert_df.show(1)
                return meta_insert_df
            return
        except Exception,e:
            raise Exception("Error while executing metadata_management_versioning() :"+str(e))
    def etl_udf_functions(self):
        """
        This conatins all the necessary type conversion udf functions that are used during DataFrame Operations
        :return:                    Does not return anything from this method.
        :type:                      None
        """
        try:
            logger.info("Inside etl_udf_functions() method ")
            self.null_val_udf = udf(lambda x: (x if (x == '' or x is None) else x.strip()) == '', BooleanType())
            self.date_func = udf(lambda x: parse(str(x)), DateType())
            self.datetime_func = udf(lambda x: parse(x), TimestampType())
            self.string_func = udf(lambda x: str(x.decode('utf-16').encode('utf-8')), StringType())
            self.int_func = udf(lambda x: int(x) if x else x, IntegerType())
            #self.float_func = udf(lambda x: float(x) if str(x).strip() else str(x).strip(), FloatType())
            self.float_func = udf(lambda x: (0.0 if (x == '' or x is None) else "{:f}".format(float(str(x).strip().replace(',','')))), FloatType())
            self.replace_comma_func = udf(lambda x: (str(x).strip().replace(',','').replace('$','')), StringType())
            # self.float_func = udf(lambda x: "{:f}".format(float(x)), FloatType())
            self.double_func = udf(lambda x: float(x) if str(x).strip() else str(x).strip(), DoubleType())
            #self.lowercase_func  = udf(lambda x: str(x)) if (str(x).strip() == '' or x is None) else (str(x.decode('utf-8')).strip()).title(), StringType())
            # import unicodedata

            def nonasciitoascii(unicodestring):
                if (unicodestring is None or unicodestring.strip()==''):
                    return
                else:
                    return unicodestring.decode('utf-8').encode("ascii","ignore").title()
            self.lowercase_func = udf(nonasciitoascii)
            def isrc_blank_to_null(isrc_string):
                if (isrc_string is None or isrc_string.strip()==''):
                    return
            self.isrc_null_check = udf(isrc_blank_to_null)

            def correct_date(datestring):
                if '/' in datestring:
                    return datestring
                elif '-' in datestring:
                    return datestring
                else:
                    return str(datetime.datetime.strptime(str(datestring), '%d%m%Y'))

            self.correct_date_func = udf(correct_date)


            # self.lowercase_func  = udf(lambda x: str(x) if (str(x) == '' or x is None) else (str(x.decode('utf-8').encode('ascii','ignore')).strip()).title(), StringType())

            return
        except Exception,e:
            raise Exception("Error while executing etl_udf_functions() :"+str(e))
    def mapping_source_to_destination(self,df,report_date_string):
        """
        This is the initial method to first map the Source file headers to the destination table headers,
        based on the metadata_property file.
        :param df:                  Data Frame of the source file that is being processed
        :type df:                   pyspark.sql.dataframe.DataFrame
        :param report_date_string:  This is to populate the Report_Date column of the Dataframe
        :return:                    Modified Dataframe
        :rtype:                     pyspark.sql.dataframe.DataFrame
        """
        try:
            logger.info("Inside mapping_source_to_destination() method ")
            for i in range(len(self.metadata_property)):
                #import pdb;pdb.set_trace()
                # if metadata_property.keys()[i] in ('Net_Payment','Quantity'):
                #     import pdb;pdb.set_trace()
                for s_header in self.metadata_property.values()[i]:
                    if '++' in s_header:
                        if self.metadata_property.keys()[i] not in df.columns:
                            comb_headers = s_header.split('++')
                            if set(comb_headers)<set(df.columns):
                                for col in comb_headers:
                                    if col in df.columns:
                                        df = df.withColumn(col, sf.when(self.null_val_udf(col), 0.0).otherwise(df[col]))
                                        df = df.withColumn(col, self.replace_comma_func(col))
                                        # print "Inside DF"
                                        # df.show()
                                        # df = df.withColumn(col, self.float_func(col))
                                # df= df.withColumn(metadata_property.keys()[i], sum(df[col].cast(FloatType()) for col in comb_headers))
                                df= df.withColumn(self.metadata_property.keys()[i], sum(df[col] for col in comb_headers))
                            # break
                    else:
                        if self.metadata_property.keys()[i] not in df.columns:
                            df= df.withColumnRenamed(s_header, self.metadata_property.keys()[i])
            df= df.withColumn('Deal', lit(self.deal_name).cast(StringType()))
            if 'Source' not in df.columns:
                df= df.withColumn('Source', lit(self.vendor_name).cast(StringType()))
            df = df.withColumn('Report_Date', lit(report_date_string).cast(StringType()))
            # df.show(5)
            return df
        except Exception,e:
            raise Exception("Error while executing mapping_source_to_destination() :"+str(e))
    def type_fill_logic(self,df):
        """
        This method used for "Track" name population from "Type" column whenever the "Track" name is empty.
        :param df:  Data Frame of the source file that is being processed
        :type df:   pyspark.sql.dataframe.DataFrame
        :return:    Data Frame after filing Type
        :rtype:     pyspark.sql.dataframe.DataFrame
        """
        try:
            logger.info("Inside type_fill_logic() method ")
            if "Type" in df.columns:
                df = df.withColumn("Track", sf.when(self.null_val_udf("Track"), col("Type")).otherwise(col("Track").alias("Track")))
            return df
        except Exception,e:
            raise Exception("Error while executing type_fill_logic() :"+str(e))
    def default_artist_play_date(self,df,report_date_string):
        """
        This method is used to populate default values for Artist and Play Date in case they are empty.
        We use Deal name as Artist name and Report Date as play date in this case.
        :param df:  Data Frame of the source file that is being processed
        :type df:   pyspark.sql.dataframe.DataFrame
        :return:    Data Frame after filing Type
        :rtype:     pyspark.sql.dataframe.DataFrame
        """
        try:
            logger.info("Inside default_artist_play_date() method ")
            if 'Artist' not in df.columns:
                df= df.withColumn('Artist', lit(self.deal_name).cast(StringType()))
            else:
                df = df.withColumn('Artist', sf.when(self.null_val_udf('Artist'), self.deal_name).otherwise(col('Artist').alias('Artist')))
            if "Play_Date" not in df.columns:
                df= df.withColumn('Play_Date', lit(report_date_string).cast(StringType()))
            else:
                df = df.withColumn("Play_Date", sf.when(self.null_val_udf("Play_Date"), col("Report_Date")).otherwise(col("Play_Date").alias("Play_Date")))
                df = df.withColumn("Play_Date", self.correct_date_func("Play_Date"))
                df = df.withColumn("Play_Date", self.date_func(df.Play_Date))
                df = df.withColumn("Play_Date", df["Play_Date"].cast(StringType()))

            source = set(df.columns)
            payments_table = self.sqlCtx.read.jdbc(url=self.url, table="Payment", properties=self.properties)
            self.payments_columns = payments_table.columns
            destination = set(self.payments_columns)
            missing = destination - source
            for i in missing:
                df= df.withColumn(i, lit(None).cast(StringType()))
            return df
        except Exception,e:
            raise Exception("Error while executing default_artist_play_date() :"+str(e))
    def sales_category_lookup(self,df):
        """
        This is to lookup the Sale Category based on the vendor name
        :param df:  Data Frame of the source file that is being processed
        :type df:   pyspark.sql.dataframe.DataFrame
        :return:    Data Frame after filing Type
        :rtype:     pyspark.sql.dataframe.DataFrame
        """
        try:
            logger.info("Inside check_file_processed() method ")
            # if "Sale_Category" not in df.columns:
            logger.info("Inside Sales_Category processing og check_file_processed() method ")
            sales_category_table = self.sqlCtx.read.jdbc(url=self.url, table="Sales_Category_Lookup", properties=self.properties) #sqlContext.read.jdbc
            sales_category_table.registerTempTable("sales_category_lookup")
            sales_category_results = self.sqlCtx.sql('SELECT * FROM sales_category_lookup WHERE vendor_name = "'+str(self.vendor_name)+'" limit 1')
            sales_category = sales_category_results.select('sales_category').collect()[0].sales_category
            df= df.withColumn('Sale_Category', lit(sales_category).cast(StringType()))
            logger.info(df.show(5))
            logger.info("Finished sales_category_lookup() with Sale Category: "+str(sales_category))
            return df
        except Exception,e:
            raise Exception("Error while executing sales_category_lookup() :"+str(e))
    def payments_table_insert(self,df,file_name):
        """
        This method is used to prepare and insert data into Payments table
        :param df:  Data Frame of the source file that is being processed
        :type df:   pyspark.sql.dataframe.DataFrame
        :return:    Data Frame after filing Type
        :rtype:     pyspark.sql.dataframe.DataFrame
        """
        try:
            logger.info("Inside payments_table_insert() method ")
            payments_insert_df = df.select(self.payments_columns)
            to_string_fields =[]
            for i, f in enumerate(payments_insert_df.schema.fields):
                if not isinstance(f.dataType, StringType):
                    to_string_fields.append(f.name)
            if to_string_fields:
                for i in to_string_fields:
                    payments_insert_df= payments_insert_df.withColumn(i,payments_insert_df[i].cast(StringType()))
            # logger.info(payments_insert_df.show(10))
            # logger.info(payments_insert_df.printSchema())

            # print file_name
            payments_insert_df = payments_insert_df.withColumn('File_Name', lit(file_name).cast(StringType()))
            # print file_name

            # # # payments_insert_df=payments_insert_df.withColumn('Artist', self.lowercase_func('Artist')).withColumn('Track', self.lowercase_func('Track')).withColumn('Album', self.lowercase_func('Album'))
            for col_name in payments_insert_df.columns:
                if col_name in self.title_case_cols:
                    payments_insert_df=payments_insert_df.withColumn(col_name, self.lowercase_func(col_name))
            payments_insert_df=payments_insert_df.withColumn("ISRC", sf.when(self.null_val_udf('ISRC'), None).otherwise(col('ISRC').alias('ISRC')))
            logger.info("After ASCII Cast:: "+str(payments_insert_df.show(10)))
            # payments_insert_df.write.jdbc(self.url,
            #       table="Payment",
            #       mode="append",
            #       properties=self.properties)
            # logger.info("Successfully inserted into payments ")
            return payments_insert_df
        except Exception,e:
            raise Exception("Error while executing payments_table_insert() :"+str(e))
    def mysqldb_song_table_update(self, song_id_list):
        """
        Here we update the song table with is_latest = 'N'.
        :param song_id_list:    Song table's ID List which needs to be  updated with is_latest = 'N'
        :type song_id_list:     list
        :return:                Does not return anything from this method.
        :rtype:                 None
        """
        try:
            logger.info("Inside mysqldb_song_table_update() method ")
            if song_id_list:
                print song_id_list
                db = MySQLdb.connect(self.mysqldb_port,self.mysqldb_user,self.mysqldb_password,self.mysqldb_database_name)
                cursor = db.cursor()
                if len(song_id_list) == 1:
                    cursor.execute("UPDATE Song set is_latest = 'N' where ID = %s "%(str(song_id_list[0])))
                else:
                    cursor.execute("UPDATE Song set is_latest = 'N' where ID in %s "%(str(tuple(song_id_list))))
                db.commit()
                db.close()
            else:
                logger.info("No update done since the song_id_list is empty")
            return
        except Exception,e:
            raise Exception("Error while executing mysqldb_song_table_update() :"+str(e))
    def song_table_insert(self,df):
        """
        This method is used to prepare and insert data into Payments table.
        Finnaly this funcation also returns the list of id's of the song table,
        for which we need to update is_latest = 'N'
        :param df:  Data Frame of the source file that is being processed
        :type df:   pyspark.sql.dataframe.DataFrame
        :return:    Song table's ID List which needs to be  updated with is_latest = 'N'
        :rtype:     list
        """
        try:
            logger.info("Inside song_table_insert() method ")
            songs_table = self.sqlCtx.read.jdbc(url=self.url, table="Song", properties=self.properties)
            for col_name in songs_table.columns:
                if col_name in self.title_case_cols:
                    songs_table=songs_table.withColumn(col_name, self.lowercase_func(col_name))
            songs_table = songs_table.withColumn('Release_Date', self.date_func('Release_Date'))
            #songs_table = songs_table.withColumn('Artist', self.lowercase_func('Artist')).withColumn('Track', self.lowercase_func('Track')).withColumn('Album', self.lowercase_func('Album')).withColumn('Release_Date', self.date_func('Release_Date'))
            songs_table_count = songs_table.count()
            df_songs = df
            if 'Release_Date' in df_songs.columns:
                df_songs = df_songs.where((df_songs.Release_Date != "") & (df_songs.Artist != "") & (df_songs.Track != "")).select('ISRC', 'Artist', 'Track', 'Album', 'Release_Date')
                df_songs_insert = df_songs.dropDuplicates()
            else:
                df_songs = df_songs.where((df_songs.Play_Date != "") & (df_songs.Artist != "") & (df_songs.Track != "")).select('ISRC', 'Artist', 'Track', 'Album', 'Play_Date').withColumnRenamed('Play_Date', 'Release_Date')
                for col_name in df_songs.columns:
                    if col_name in self.title_case_cols:
                        df_songs=df_songs.withColumn(col_name, self.lowercase_func(col_name))
                df_songs = df_songs.withColumn('Release_Date', self.date_func('Release_Date'))
                #df_songs = df_songs.withColumn('Artist', self.lowercase_func('Artist')).withColumn('Track', self.lowercase_func('Track')).withColumn('Album', self.lowercase_func('Album')).withColumn('Release_Date', self.date_func('Release_Date'))
                df_songs_grby = df_songs.groupBy('ISRC','Artist', 'Track', 'Album').agg(sf.min('Release_Date')).withColumnRenamed('min(Release_Date)', 'Release_Date')
                # cond = ['Artist', 'Track', 'Album']
                # df_songs_join = df_songs_grby.join(df_songs, cond, 'left').drop(df_songs.Release_Date).withColumnRenamed('min(Release_Date)', 'Release_Date')
                df_songs_insert = df_songs_grby.dropDuplicates()

            df_songs_insert = df_songs_insert.withColumn('is_latest', lit('Y').cast(StringType()))
            df_songs_insert = df_songs_insert.withColumn('Album',sf.when(sf.isnull('Album'), lit(' ')).otherwise(df_songs_insert.Album))
            df_songs_insert = df_songs_insert.withColumn('ISRC',sf.when(sf.isnull('ISRC'), lit(' ')).otherwise(df_songs_insert.ISRC))

            # df_songs_insert.show(200)
            if songs_table_count == 0:
                #df_songs_insert.write.jdbc(self.url, table="Song", mode="append", properties=self.properties)
                songId_is_not_latest =[]
                return df_songs_insert, songId_is_not_latest

            else:
                df_songs_renamed = df_songs_insert.withColumnRenamed('Artist', 'dfArtist').withColumnRenamed('Track', 'dfTrack').withColumnRenamed('Album', 'dfAlbum').withColumnRenamed('ISRC', 'dfISRC').withColumnRenamed('Release_Date', 'dfRelease_Date').withColumnRenamed('is_latest', 'dfis_latest')
                # conds = [songs_table.Artist == df_songs_renamed.dfArtist, songs_table.Track == df_songs_renamed.dfTrack, songs_table.Album == df_songs_renamed.dfAlbum]
                # conds = [songs_table.ISRC == df_songs_renamed.dfISRC, songs_table.Artist == df_songs_renamed.dfArtist, songs_table.Track == df_songs_renamed.dfTrack]
                # songs_table.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/nithish/Downloads/songstable.csv")
                # df_songs_insert.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/nithish/Downloads/currentfile.csv")
                songs_table = songs_table.where(songs_table.is_latest == 'Y')

                songs_table = songs_table.withColumn('Album',sf.when(sf.isnull('Album'), lit(' ')).otherwise(songs_table.Album))
                songs_table = songs_table.withColumn('ISRC',sf.when(sf.isnull('ISRC'), lit(' ')).otherwise(songs_table.ISRC))
                df_songs_renamed = df_songs_renamed.withColumn('dfAlbum',sf.when(sf.isnull('dfAlbum'), lit(' ')).otherwise(df_songs_renamed.dfAlbum))
                df_songs_renamed = df_songs_renamed.withColumn('dfISRC',sf.when(sf.isnull('dfISRC'), lit(' ')).otherwise(df_songs_renamed.dfISRC))

                # songs_table = songs_table.withColumn('ISRC',sf.when(self.null_val_udf('ISRC'), lit('-')).otherwise(songs_table.ISRC))
                # df_songs_renamed = df_songs_renamed.withColumn('dfISRC',sf.when(self.null_val_udf('dfISRC'), lit('-')).otherwise(df_songs_renamed.dfISRC))

                # songs_table_filled = songs_table.na.fill('-')
                # df_songs_filled = df_songs_renamed.na.fill('-')

                # print "Songs Table"
                # songs_table.show(1000)
                # print "Current DF"
                # df_songs_renamed.show(1000)

                df_songs_table = songs_table.join(df_songs_renamed, (songs_table.ISRC == df_songs_renamed.dfISRC) & (songs_table.Artist == df_songs_renamed.dfArtist) & (songs_table.Track == df_songs_renamed.dfTrack) & (songs_table.Album == df_songs_renamed.dfAlbum), 'fullouter')
                # df_songs_table = songs_table.join(df_songs_renamed, conds, 'fullouter')
                # df_songs_table.show(1000)
                # print "joined"
                df_songs_table_append = df_songs_table.where(sf.isnull(df_songs_table.ISRC) & sf.isnull(df_songs_table.Track) & sf.isnull(df_songs_table.Artist) & sf.isnull(df_songs_table.Album) & sf.isnull(df_songs_table.Release_Date) & sf.isnull(df_songs_table.is_latest) & sf.isnull(df_songs_table.id))
                # print "filtering"
                # df_songs_table_append.show(1000)
                df_songs_table_append = df_songs_table_append.drop('id').drop('Artist').drop('Album').drop('Track').drop('ISRC').drop('Release_Date').drop('is_latest')
                df_songs_table_append = df_songs_table_append.withColumnRenamed('dfArtist', 'Artist').withColumnRenamed('dfTrack', 'Track').withColumnRenamed('dfAlbum', 'Album').withColumnRenamed('dfISRC', 'ISRC').withColumnRenamed('dfRelease_Date', 'Release_Date').withColumnRenamed('dfis_latest', 'is_latest')
                # print "col renamed"
                # df_songs_table_append.show(1000)
                df_songs_table_append = df_songs_table_append.dropDuplicates()
                # print "dup dropped"
                # df_songs_table_append.show(1000)
                # if df_songs_table_append:
                #     logger.info(df_songs_table_append.show(1))
                #df_songs_table_append.write.jdbc(self.url, table="Song", mode="append", properties=self.properties)
                min_date_udf = udf(lambda x, y: 0 if (x <= y) else 1, IntegerType())
                ISRC_null_check_udf = udf(lambda x: 1 if (x == '' or x is None) else 0, IntegerType())
                df_songs_table_update = df_songs_table.where(df_songs_table.ISRC.isNotNull() & df_songs_table.Track.isNotNull() & df_songs_table.Artist.isNotNull() & df_songs_table.Album.isNotNull() & df_songs_table.Release_Date.isNotNull() & df_songs_table.is_latest.isNotNull())
                df_songs_table_update = df_songs_table_update.where(df_songs_table_update.dfISRC.isNotNull() & df_songs_table_update.dfTrack.isNotNull() & df_songs_table_update.dfArtist.isNotNull() & df_songs_table_update.dfAlbum.isNotNull() & df_songs_table_update.dfRelease_Date.isNotNull() & df_songs_table_update.dfis_latest.isNotNull())
                df_songs_table_update = df_songs_table_update.drop(df_songs_table_update.dfTrack).drop(df_songs_table_update.dfArtist).drop(df_songs_table_update.dfAlbum)
                # print "up filtered"
                # df_songs_table_update.show(1000)
                df_songs_table_update = df_songs_table_update.withColumn('ISRCFlag', ISRC_null_check_udf('ISRC'))
                df_songs_table_update = df_songs_table_update.withColumn('dfISRCFlag', ISRC_null_check_udf('dfISRC'))
                df_songs_table_update = df_songs_table_update.withColumn('updateISRCFlag', sf.when((df_songs_table_update.ISRCFlag == 1) & (df_songs_table_update.dfISRCFlag == 0), 1).otherwise(0))
                df_songs_table_update = df_songs_table_update.withColumn("ISRC", sf.when(sf.isnull("ISRC"), df_songs_table_update.dfISRC).otherwise(df_songs_table_update.ISRC).alias("ISRC"))
                df_songs_table_update = df_songs_table_update.withColumn('updateReleaseDateFlag', min_date_udf('Release_Date', 'dfRelease_Date'))
                # print "up flagged"
                # df_songs_table_update.show(1000)
                df_songs_table_update = df_songs_table_update.where((df_songs_table_update.updateReleaseDateFlag == 1) | (df_songs_table_update.updateISRCFlag ==1))
                # print "up to be updated filter"
                # df_songs_table_update.show(1000)
                df_songs_table_update = df_songs_table_update.withColumn("ISRC", sf.when(df_songs_table_update.updateISRCFlag ==1, df_songs_table_update.dfISRC).otherwise(df_songs_table_update.ISRC).alias("ISRC"))
                df_songs_table_update = df_songs_table_update.withColumn("Release_Date", sf.when(df_songs_table_update.updateReleaseDateFlag ==1, df_songs_table_update.dfRelease_Date).otherwise(df_songs_table_update.Release_Date).alias("Release_Date"))
                df_songs_table_update = df_songs_table_update.drop('dfis_latest').drop('ISRCFlag').drop('dfISRCFlag').drop('updateISRCFlag').drop('dfISRC').drop('dfRelease_Date').drop('updateReleaseDateFlag')
                df_id = df_songs_table_update
                songId_is_not_latest = df_id.select('id').rdd.flatMap(lambda x: x).collect()
                print songId_is_not_latest
                df_songs_table_update = df_songs_table_update.drop('id')
                df_songs_table_update = df_songs_table_update.dropDuplicates()
                # if df_songs_table_update:
                #     logger.info(df_songs_table_update.show(1))
                #df_songs_table_update.write.jdbc(self.url, table="Song", mode="append", properties=self.properties)

                def unionAll(*dfs):
                    return reduce(DataFrame.unionAll, dfs)
                song_insert_df=unionAll(df_songs_table_append,df_songs_table_update)
                print "before union all print"
                song_insert_df.show(1000)

                return song_insert_df, songId_is_not_latest
        except Exception,e:
            raise Exception("Error while executing song_table_insert() :"+str(e))
    def jdbc_write(self,df,table_name):
        try:
            df.write.jdbc(url=self.url, table=table_name, mode="append", properties=self.properties)
            return
        except Exception,e:
            raise Exception("Exception in jdbc_write for table : "+str(table_name)+" \n "+str(e))
    def update_file_processed_table(self,file_name,payments_insert_df):
        """
        This is the final stage of processing the source file.
        Once all the table updates are successful we this processed file name into the File_Summary table.
        :param file_name:   The file_name that was successfully processed
        :type file_name:    str
        :return:            Does not return anything from this method.
        :rtype:             None
        """
        try:
            logger.info("Inside song_table_updates() method ")
            file_tot_net_pymnts = payments_insert_df.agg(sf.sum('Net_Payment').alias('Total_Net_Payment'))

            #File Processed write
            processed_file_insert_df = self.sqlCtx.createDataFrame([(self.deal_name,self.vendor_name,file_name,str(datetime.datetime.now().date()),file_tot_net_pymnts.select('Total_Net_Payment').take(1)[0][0])], ['deal_name', 'vendor_name','file_name','processed_date','total_net_payment'])
            processed_file_insert_df.write.jdbc(url=self.url,
                               table="File_Summary",
                               mode="append",
                               properties={"driver": 'com.mysql.jdbc.Driver'})
            return
        except Exception,e:
            raise Exception("Error while executing update_file_processed_table() :"+str(e))

def main(sqlCtx,file_path,property_file_path,logger):
    """
    Step 1: This main function first reads all the .csv files from the file_path.
    Step 2: Iterates through each file and checks if the file is already processed if not then it continues
     to call each function of the ETL_Processing class using the class object etl_obj.
    :param sqlCtx:              Pyspark sql context
    :type sqlCtx:               pyspark.sql.context.SQLContext
    :param file_path:           File path of the source files
    :type file_path:            str
    :param property_file_path:  Property file path
    :type property_file_path:   str
    :param logger:              Logger Object
    :return:            Does not return anything from this method.
    :rtype:             None
    """
    try:
        logger.info("Inside Main Function")
        mysqldb_port = "soundhousemasterdb.c4q9tpcfjner.us-east-2.rds.amazonaws.com"
        mysqldb_user = "dbadmin"
        mysqldb_password = "dbadmin123"
        mysqldb_database_name = "SoundHouse"
        etl_obj = ETL_Processing(sqlCtx,property_file_path,logger)
        fs = s3fs.S3FileSystem(anon=True)

        etl_obj.deal_vendor_parsing(file_path)
        source_file_list=[]
        for i in  fs.ls('soundhousellc/'+str(file_path.split('soundhousellc/')[1])):
            if i.split('.')[-1]=='csv':
                source_file_list.append(i)
        #source_file_list= glob.glob(file_path+'*.csv')
        logger.info("Source Files List to be processed : "+str(source_file_list))
        if source_file_list:
            for source_file in source_file_list:
                try:
                    source_file_path= source_file.split('/')
                    file_name = source_file_path[-1]
                    processed_flag = etl_obj.check_file_processed(file_name)
                    if not processed_flag:
                        logger.info("File being processed : "+str(file_name))
                        report_date, report_date_string = etl_obj.report_date_parsing(file_name)
                        df = (sqlCtx.read.format("com.databricks.spark.csv")
                                          .option("header", "true")
                                          .option("charset", "utf-8")
                                          .load("s3n://"+source_file))
                        print "File being read................"

                        def nonasciireplacer(text):
                            return ''.join(i for i in text if ord(i) < 128).strip()

                        replacer = udf(nonasciireplacer)

                        for col_name in df.columns:
                            if '.' in col_name:
                                df = df.withColumnRenamed(col_name, col_name.replace('.', ' '))

                        for col_name in df.columns:
                            df = df.withColumn(col_name, replacer(col_name))

                        # df.show()
                        etl_obj.etl_udf_functions()
                        meta_insert_df= etl_obj.metadata_management_versioning(df,report_date_string)
                        df = etl_obj.mapping_source_to_destination(df,report_date_string)
                        df = etl_obj.type_fill_logic(df)
                        df = etl_obj.default_artist_play_date(df,report_date_string)
                        df = etl_obj.sales_category_lookup(df)
                        payments_insert_df = etl_obj.payments_table_insert(df,file_name)
                        song_insert_df,songId_list_toUpdate= etl_obj.song_table_insert(df)

                        payments_insert_df.show(1)
                        # song_insert_df.show(1)
                        if meta_insert_df:
                            etl_obj.jdbc_write(meta_insert_df,"Metadata_Management")
                        etl_obj.jdbc_write(payments_insert_df,"Payment")
                        etl_obj.jdbc_write(song_insert_df,"Song")
                        etl_obj.mysqldb_song_table_update(songId_list_toUpdate)
                        etl_obj.update_file_processed_table(file_name,payments_insert_df)
                        logger.info("Completed file processing")
                    else:
                        logger.info("The file '%s' has already been processed "%(str(file_name)))
                except Exception,e:
                    logger.info("Exception while processing file : "+str(source_file)+ "\nFailed with exception :"+str(e))
                    db = MySQLdb.connect(mysqldb_port,mysqldb_user,mysqldb_password,mysqldb_database_name)
                    cursor = db.cursor()
                    cursor.execute("INSERT INTO Error_Summary VALUES ('%s' , '%s', '%s') "%(str(source_file),str(e),str(datetime.datetime.now().date())))
                    db.commit()
                    db.close()
                    continue
        return
    except Exception,e:
        logger.info("Error while executing main() :"+str(e))


if __name__ == '__main__':
    """
    This is the entry point into this code from the spark-submit.

    Example which is run in shell script:

    ~$ bin/spark-submit  /home/rahul/Downloads/Sound_House/Code_design/aws_main.py
                   '/home/rahul/Downloads/Sound_House/Code_design/Source_Files/Anthem Lights/SX-RO/'
                   '/home/rahul/Downloads/Sound_House/Code_design/property_file.py'

    We read the two arguments that are passed after "aws_main.py".
    The first path refers to source files that needs to be processed which contains <path>/<Deal Name>/<Vendor Name>/
    In the above example '/home/rahul/Downloads/Sound_House/Code_design/Source_Files/Anthem Lights/SX-RO/'
    Here we must endure the last two directories denote "/<Deal Name>/<Vendor Name>/"
    Here in our example Deal Name is  "Anthem Lights"
    and Vendor name is "SX-RO"

    Here we initialise the pyspark SQL context.
    Call the main() function by passing sqlContext file path and property file path.

    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    # reload(sys)
    # sys.setdefaultencoding('utf-8')
    # import pyspark
    sc = pyspark.SparkContext()
    # from pyspark.sql import SQLContext
    sqlCtx = SQLContext(sc)
    #conf = SparkConf().setAppName("app")
    #sc = SparkContext(conf=conf)
    file_path = sys.argv[1]
    property_file_path = sys.argv[2]
    print(file_path)
    print(property_file_path)
    #len(sys.argv)<2
    main(sqlCtx,file_path,property_file_path,logger)
