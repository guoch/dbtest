# Databricks notebook source
Seq("""{"a": 1, "b": 2}""", """{bad-record""").toDF().write.text("/tmp/input/jsonFile")

val df = spark.read
  .option("badRecordsPath", "/tmp/badRecordsPath")
  .schema("a int, b int")
  .json("/tmp/input/jsonFile")

df.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh ls -l /dbfs/databricks/init/eqr_anaconda_init_2.sh

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks/init/eqr_anaconda_init_2.sh

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC #set -ex
# MAGIC . /databricks/conda/etc/profile.d/conda.sh
# MAGIC conda activate /databricks/python
# MAGIC conda install -y py-xgboost feather-format keras scrapy spacy cudatoolkit cudnn pulp pytorch torchvision cudatoolkit=10.0
# MAGIC /databricks/python/bin/pip install kedro
# MAGIC /databricks/python/bin/pip install epydoc

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/databricks/init_scripts

# COMMAND ----------

#global init script
%sh cat /dbfs/databricks/init/eqr_anaconda_init_2.sh

# COMMAND ----------

# MAGIC %sh rm -f /dbfs/databricks/init/eqr_anaconda_init_2.sh

# COMMAND ----------

# MAGIC %sh ls -l /databricks/python/bin

# COMMAND ----------

dbutils.fs.put("dbfs:/databricks/init_scripts/anaconda_init.sh","""
#!/bin/bash
time_start=`date +%s`
set -ex
. /databricks/conda/etc/profile.d/conda.sh
conda activate databricks-standard
conda config --append channels conda-forge
conda config --append channels anaconda
conda config --append channels pytorch
conda install -y py-xgboost feather-format keras scrapy spacy cudatoolkit cudnn pulp pytorch torchvision cudatoolkit=10.0
pip install kedro
pip install epydoc
time_end=`date +%s`
time_exec=`expr $(( $time_end - $time_start ))`
echo "Execution time is $time_exec seconds"
""",True)

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks/init_scripts/anaconda_init.sh

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/databricks/init/midtermrpackages

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks/init/midtermrpackages/extractRpkgsDBR60.sh

# COMMAND ----------

# MAGIC %sh /databricks/python/bin/pip list | egrep 'py-xgboost|cudatoolkit'

# COMMAND ----------

# MAGIC %sh ls -l '/dbfs/databricks/init/output/chgml_F conda init/2019-09-30_11-40-56'

# COMMAND ----------

# MAGIC %sh cat '/dbfs/databricks/init/output/chgml_F conda init/2019-09-30_10-44-05/anaconda_init.sh_10.10.12.6.log'

# COMMAND ----------

# MAGIC %sh cat '/dbfs/databricks/init/output/chgml_F conda init/2019-09-30_10-04-03/anaconda_init.sh_10.10.12.7.log'

# COMMAND ----------

# MAGIC %sh 
# MAGIC conda info --envs

# COMMAND ----------

# MAGIC %sh cat /databricks/conda/etc/profile.d/conda.sh

# COMMAND ----------

# MAGIC %sh date

# COMMAND ----------

