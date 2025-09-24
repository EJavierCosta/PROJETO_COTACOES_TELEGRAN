# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class ingestao:
    def __init__(self, tabela, schema, catalog, dbpath):
        self.tabela = tabela
        self.schema = schema
        self.catalog = catalog
        self.dbpath = dbpath
        

    def carga(self):
        db = (spark.read
                        .format("delta")
                        .table(self.dbpath)
                        )   
        return db
    
    def salva(self, db):
        (db.writeTo(f"{self.catalog}.{self.schema}.{self.tabela}")
           .tableProperty("delta.autoOptimize.optimizeWrite", "true")
           .tableProperty("delta.autoOptimize.autoCompact", "true")
           .tableProperty("delta.enableChangeDataFeed", "true")
           .using("delta")
           .createOrReplace()
           )  
        return True
    
    def run(self):
        db = self.carga()
        return self.salva(db)
    
class ingestaoCDC (ingestao):
    def __init__(self, tabela, schema, catalog, dbpath, checkpointpath):
        super().__init__(tabela, schema, catalog, dbpath)
        self.checkpointpath = checkpointpath
        

    def settabela (self):
        bronze = delta.DeltaTable.forName(spark, f"{self.catalog}.{self.schema}.{self.tabela}")
        return bronze

    def upsert (self, df, bronze):
        # Filtra os tipos de mudança que não interessam
        df_filtered = df.filter(col("_change_type") != 'update_preimage')

        # Define o registro mais recente por Símbolo
        windowSpec = Window.partitionBy("Simbolo").orderBy(desc("_commit_timestamp"), desc("_commit_version"))

        # Pega a última atualização para cada Símbolo
        df_cotacoes_cdc = (df_filtered.withColumn("row_num", row_number().over(windowSpec))
                                        .filter(col("row_num") == 1)
                                        .drop("row_num"))

        (bronze.alias("b")
             .merge(df_cotacoes_cdc.alias("c"),"b.Simbolo = c.Simbolo")
             .whenMatchedDelete(condition = "c._change_type = 'delete'")
             .whenMatchedUpdateAll(condition = "c._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "c._change_type = 'insert' OR c._change_type = 'update_postimage'")  
             .execute()
             )

    def leituraCDC (self):
        tabela_cdc= (spark.readStream
                               .format("delta")
                               .option("readChangeFeed", "true")
                               .option("startingVersion", 0)  
                               .table(self.dbpath)
                               )
        return tabela_cdc
        
    def salvaCDC (self, tabela_cdc, bronze):
        stream = (tabela_cdc.writeStream
                      .trigger(availableNow=True)
                      .option("checkpointLocation", self.checkpointpath)
                      .foreachBatch(lambda df, batchId: self.upsert(df, bronze))
                      )
        return stream.start()
    
    def runCDC (self):
        bronze = self.settabela()
        tabela_cdc = self.leituraCDC()
        return self.salvaCDC(tabela_cdc, bronze)
