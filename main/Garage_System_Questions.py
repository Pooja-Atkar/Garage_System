from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import expr
from pyspark.sql.window import Window
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Mock_Practice") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.Ui.port", "4040") \
        .getOrCreate()

    custm_df = spark.read.csv(r"D:\PythonSparkProject\Garage_System_Input_Files\customer.csv", header=True)
    # custm_df.show()

    vendor_df = spark.read.csv(r"D:\PythonSparkProject\Garage_System_Input_Files\vendors.csv", header=True)
    # vendor_df.show()

    emp_df = spark.read.csv(r"D:\PythonSparkProject\Garage_System_Input_Files\employee.csv", header=True)
    # emp_df.show()

    sparepart_df = spark.read.csv(r"D:\PythonSparkProject\Garage_System_Input_Files\sparepart.csv", header=True)
    # sparepart_df.show()

    purchase_df = spark.read.csv(r"D:\PythonSparkProject\Garage_System_Input_Files\purchase.csv", header=True)
    # purchase_df.show()

    ser_det_df = spark.read.csv(r"D:\PythonSparkProject\Garage_System_Input_Files\ser_det.csv", header=True)
    # ser_det_df.show()

    # #####################################################################################
    # Q.1  List all the customers serviced.
    df1 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
                       how='inner').select([custm_df.CID, custm_df.CNAME, ser_det_df.CID])
    # df1.show()
    # Another Way
    # custm_df.join(ser_det_df, custm_df.CID == ser_det_df.CID, "inner").show(truncate=False)

    # Q.2  Customers who are not serviced.  # Incomplete
    df2 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
                        how='inner').select([custm_df.CID, custm_df.CNAME, ser_det_df.CID]). \
                        filter(custm_df.CID.isin(ser_det_df.CID))
    # df2.show()

    #Q.3  Employees who have not received the commission.
    df3 = emp_df.join(ser_det_df, on=emp_df.EID == ser_det_df.EID,
                    how='inner').select([emp_df.EID, emp_df.ENAME, ser_det_df.EID,ser_det_df.COMM]). \
                    filter(ser_det_df.COMM == 0)
    df3.show()

    # Q.4  Name the employee who have maximum Commission. #Incomplete
    df4 = emp_df.join(ser_det_df, on=emp_df.EID == ser_det_df.EID,
                      how='inner').select([emp_df.EID, emp_df.ENAME, ser_det_df.EID])

    df4.show()

    # Q.5  Show employee name and minimum commission amount received by an employee.#Incomplete
    df5 = emp_df.join(ser_det_df, on=emp_df.EID == ser_det_df.EID,
                      how='inner').select([emp_df.EID, emp_df.ENAME, ser_det_df.EID, ser_det_df.COMM]). \
        filter(ser_det_df.COMM == 0)
    df5.show()

    # Q.6  Display the Middle record from any table. #Incomplete

    # Q.7  Display last 4 records of any table. #Incomplete
    # Q.8  Count the number of records without count function from any table.#Incomplete
    # Q.9  Delete duplicate records from "Ser_det" table on cid.(note Please rollback after execution).Incomplete

    # Q.10 Show the name of Customer who have paid maximum amount
    









#


