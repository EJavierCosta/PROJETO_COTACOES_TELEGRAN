from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class ingestao:
    #Full Load
    def __init__(self, spark, tabela, schema, catalog, dbpath):
        self.spark = spark
        self.tabela = tabela
        self.schema = schema
        self.catalog = catalog
        self.dbpath = dbpath
     
    def carga(self):
        db = (self.spark.read
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
    
    #CDF spark + window

    def __init__(self, spark, tabela, schema, catalog, dbpath, checkpointpath):
        super().__init__(spark, tabela, schema, catalog, dbpath)
        self.checkpointpath = checkpointpath
        
    def settabela (self):
        tabela = DeltaTable.forName(self.spark, f"{self.catalog}.{self.schema}.{self.tabela}")
        return tabela

    def upsert (self, df, tabela):
        # Filtra os tipos de mudança que não interessam
        df_filtered = df.filter(col("_change_type") != 'update_preimage')

        # Define o registro mais recente por Símbolo
        windowSpec = (Window.partitionBy("Simbolo")
                            .orderBy(desc("_commit_timestamp"), desc("_commit_version"))
                            )

        # Pega a última atualização para cada Símbolo
        df_cotacoes_cdc = (df_filtered.withColumn("row_num", row_number().over(windowSpec))
                                      .filter(col("row_num") == 1)
                                      .drop("row_num")
                                      )

        (tabela.alias("b")
               .merge(df_cotacoes_cdc.alias("c"),"b.Simbolo = c.Simbolo")
               .whenMatchedDelete(condition = "c._change_type = 'delete'")
               .whenMatchedUpdateAll(condition = "c._change_type = 'update_postimage'")
               .whenNotMatchedInsertAll(condition = "c._change_type = 'insert' OR c._change_type = 'update_postimage'")  
               .execute()
               )

    def leituraCDC (self):
        tabela_cdc= (self.spark.readStream
                               .format("delta")
                               .option("readChangeFeed", "true")
                               .option("startingVersion", 0)  
                               .table(self.dbpath)
                               )
        return tabela_cdc
        
    def salvaCDC (self, tabela_cdc, tabela):
        stream = (tabela_cdc.writeStream
                            .trigger(availableNow=True)
                            .option("checkpointLocation", self.checkpointpath)
                            .foreachBatch(lambda df, batchId: self.upsert(df, tabela))
                            )
        return stream.start()
    
    def runCDC (self):
        tabela = self.settabela()
        tabela_cdc = self.leituraCDC()
        return self.salvaCDC(tabela_cdc, tabela)
    
    
class ingestaoCDF (ingestaoCDC):
    
    #CDF spark + SQL

    def __init__(self, spark, tabela, schema, catalog, dbpath, checkpointpath):
        super().__init__(self, spark, tabela, schema, catalog, dbpath, checkpointpath)
        
        
    def settabela (self):
        silver = DeltaTable.forName(self.spark, f"{self.catalog}.{self.schema}.{self.tabela}")
        return silver

    def upsert (self, df, silver):
        df.createOrReplaceGlobalTempView(f"silver_{self.tablename}")

        query = f"""
        SELECT *
        FROM global_temp.silver_{self.tablename}
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY Simbolo ORDER BY _commit_timestamp DESC) = 1
        """
        df_cdf = self.spark.sql(query)
        df_upsert = self.spark.sql(self.query, df=df_cdf)

        (self.silver
             .alias("s")
             .merge(df_upsert.alias("d"), f"s.{self.id_field} = d.{self.id_field}") 
             .whenMatchedDelete(condition = "d._change_type = 'delete'")
             .whenMatchedUpdateAll(condition = "d._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "d._change_type = 'insert' OR d._change_type = 'update_postimage'")
               .execute())

    def execute(self):
        df = self.load()
        return self.save(df)

    def leituraCDC (self):
        tabela_cdc= (self.spark.readStream
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