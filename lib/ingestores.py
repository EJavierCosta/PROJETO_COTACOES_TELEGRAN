from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import re
import utils

class ingestao:
    #Full Load
    def __init__(self, spark, tabela, schema, catalog, path_origem):
        self.spark = spark
        self.tabela = tabela
        self.schema = schema
        self.catalog = catalog
        self.path_origem = path_origem
     
    def carga(self):
        db = (self.spark.read
                        .format("delta")
                        .table(self.path_origem)
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

    def __init__(self, spark, tabela, schema, catalog, path_origem, checkpointpath):
        super().__init__(spark, tabela, schema, catalog, path_origem)
        self.checkpointpath = checkpointpath
        
        
    def settabela (self):
        tabela_destino = DeltaTable.forName(self.spark, f"{self.catalog}.{self.schema}.{self.tabela}")
        return tabela_destino

    def upsert (self, df, tabela_destino):
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
        (tabela_destino.alias("b")
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
                               .table(self.path_origem)
                               )
        return tabela_cdc
        
    def salvaCDC (self, tabela_cdc, tabela_destino, funcao):
        stream = (tabela_cdc.writeStream
                            .trigger(availableNow=True)
                            .option("checkpointLocation", self.checkpointpath)
                            .foreachBatch(lambda df, batchId: funcao(df, tabela_destino))
                            )
        return stream.start()
    
    def runCDC (self):
        tabela_destino = self.settabela()
        tabela_cdc = self.leituraCDC()
        return self.salvaCDC(tabela_cdc, tabela_destino, self.upsert)
    

    
class ingestaoCDF (ingestaoCDC):

    def __init__(self, spark, tabela, schema, catalog, path_origem, checkpointpath, query):
        super().__init__(spark, tabela, schema, catalog, path_origem, checkpointpath)
        self.query = query

    def upsert_sql (self, df, tabela_destino):
        df.createOrReplaceTempView("df_temp")
        query_filtro = f"""
        SELECT *
        FROM df_temp
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY Simbolo ORDER BY _commit_timestamp DESC) = 1
        """

        df_cdf = self.spark.sql(query_filtro)     
        df_cdf.createOrReplaceTempView("df")
        query_modificada = trata_query(self.query)
        df_cdf_atualizado = self.spark.sql(query_modificada)

        (tabela_destino.alias("a")
                       .merge(df_cdf_atualizado.alias("b"), f"a.TIKER = b.TIKER") 
                       .whenMatchedDelete(condition = "b._change_type = 'delete'")
                       .whenMatchedUpdateAll(condition = "b._change_type = 'update_postimage'")
                       .whenNotMatchedInsertAll(condition = "b._change_type = 'insert' OR b._change_type = 'update_postimage'")
                       .execute()
                       )
    def runCDF (self):
        tabela_destino = self.settabela()
        tabela_cdc = self.leituraCDC()
        return self.salvaCDC(tabela_cdc, tabela_destino, self.upsert_sql)
