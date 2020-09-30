import pyodbc
from .items import ArAutomationItem
from .items import ArAutomationItemHistory
from .items import ArAutomationItemPerforation
from .items import ArAutomationItemStimulation
from .items import ArAutomationItemProduction
from .items import ArAutomationItemInjection
from .items import ArAutomationItemInspection


class ArAutomationPipeline(object):
    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = pyodbc.connect('your_pyodbc_con_string')
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_WellDetailsRaw' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_WellDetailsRaw;""")
        self.cursor.execute("""CREATE TABLE AR_WellDetailsRaw
        (SNo int identity,  [Permit_Number] NVARCHAR (500), [Lease_Name] NVARCHAR (500), [Well_Type] NVARCHAR (500),
        [Comments] NVARCHAR (4000), [Operator] NVARCHAR (500), [Well_Name] NVARCHAR (500), [Well_Status] NVARCHAR (500),
        [API] NVARCHAR (500), [Well_No] NVARCHAR (500), [District] NVARCHAR (500), [API_Well_Number] NVARCHAR (500),
        [County] NVARCHAR (500), [Field] NVARCHAR (500), [Well_Type_] NVARCHAR (500), [Ground_Elevation] NVARCHAR (500),
        [Latitude] NVARCHAR (500), [Status_Date] NVARCHAR (500), [Footage_N_S] NVARCHAR (500),[Township] NVARCHAR (50),
        [TownshipDir] NVARCHAR (50), [Permit] NVARCHAR (500), [Zone_Pool] NVARCHAR (500), [Total_Depth] NVARCHAR (500),
        [Well_Status_] NVARCHAR (500), [Plugback_Total_Depth] NVARCHAR (500), [Longitude] NVARCHAR (500),
        [Section] NVARCHAR (500), [Footage_E_W] NVARCHAR (500), [Range] NVARCHAR (50),[RangeDir] NVARCHAR (50))""")
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_History' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_History;""")
        self.cursor.execute(""" CREATE TABLE AR_History
        (SNo int identity, [API_Well_No] NVARCHAR(50), [Type_Work] NVARCHAR(100), [Effective_Date] NVARCHAR(50),
        [Comments] NVARCHAR(100))""")
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_Perforation' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_Perforation""")
        self.cursor.execute("""CREATE TABLE AR_Perforation
        (SNo int identity, [API_Well_No] NVARCHAR(50),[Permit] NVARCHAR(50), [Top] NVARCHAR(10), [Bottom] NVARCHAR(10),
        [Date_Perf'ed] NVARCHAR(50),[Size] NVARCHAR (50),[Num_Shots] NVARCHAR(50),[Spacing] NVARCHAR(50),
        [Pool_Name] NVARCHAR(50),[Comments] NVARCHAR(500))""")
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_Stimulation' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_Stimulation;""")
        self.cursor.execute("""CREATE TABLE AR_Stimulation
        (SNo int identity, [API_Well_No] NVARCHAR(50),[Permit] NVARCHAR(50), [Date_Treated] NVARCHAR(50),
        [Top] NVARCHAR(10),[Bottom] NVARCHAR(10), [Hole_Type] NVARCHAR(50),[Fluid_Type] NVARCHAR (50),
        [Fluid_Amount] NVARCHAR(50), [Fluid_Unit] NVARCHAR(50),[Comment] NVARCHAR(500))""")
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_Production' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_Production;""")
        self.cursor.execute("""CREATE TABLE AR_Production
        (SNo int identity, [API_Well_No] NVARCHAR(50),[PRU_ID] NVARCHAR(50), [Report_Date] NVARCHAR(50),
        [Lease_PRU_Name] NVARCHAR(500),[Oil_Bbls] NVARCHAR(50),[Gas_Prod_MCF] NVARCHAR(50),[Water_Bbls] NVARCHAR(50),
        [Gas_Sales_MCF]  NVARCHAR(50))""")
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_Injection' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_Injection;""")
        self.cursor.execute("""CREATE TABLE AR_Injection
        (SNo int identity, [API_Well_No] NVARCHAR(50),[UIC_Permit] NVARCHAR(50), [Report_Date] NVARCHAR(50),
        [Inj_Days] NVARCHAR(50),[Vol_Liquid] NVARCHAR(50),[Vol_Gas] NVARCHAR(50),[Max_Inj_Rate] NVARCHAR(50),
        [Avg_Tbg_Press] NVARCHAR(50),[Max_Tbg_Press] NVARCHAR(50))""")
        self.cursor.execute("""if exists (select * from INFORMATION_SCHEMA.TABLES where
        TABLE_NAME = 'AR_Inspection' AND TABLE_SCHEMA = 'dbo') drop table dbo.AR_Inspection;""")
        self.cursor.execute("""CREATE TABLE AR_Inspection
        (SNo int identity, [API_Well_No] NVARCHAR(50),[Permit] NVARCHAR(50),[Form] NVARCHAR(50),
        [Inspect_Date] NVARCHAR(50))""")

    def process_item(self, item, spider):
        if isinstance(item, ArAutomationItem):
            return self.handle_wells(item, spider)
        if isinstance(item, ArAutomationItemHistory):
            return self.handle_history(item, spider)
        if isinstance(item, ArAutomationItemPerforation):
            return self.handle_perforation(item, spider)
        if isinstance(item, ArAutomationItemStimulation):
            return self.handle_stimulation(item, spider)
        if isinstance(item, ArAutomationItemProduction):
            return self.handle_production(item, spider)
        if isinstance(item, ArAutomationItemInjection):
            return self.handle_injection(item, spider)
        if isinstance(item, ArAutomationItemInspection):
            return self.handle_inspection(item, spider)

    def handle_wells(self, item, spider):
        self.store_database_wells(item)
        return item

    def handle_history(self, item, spider):
        self.store_database_history(item)
        return item

    def handle_perforation(self, item, spider):
        self.store_database_perforation(item)
        return item

    def handle_stimulation(self, item, spider):
        self.store_database_stimulation(item)
        return item

    def handle_production(self, item, spider):
        self.store_database_production(item)
        return item

    def handle_injection(self, item, spider):
        self.store_database_injection(item)
        return item

    def handle_inspection(self, item, spider):
        self.store_database_inspection(item)
        return item

    def store_database_wells(self, item):
        self.cursor.execute("""INSERT INTO AR_WellDetailsRaw
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            item['Permit_Number'], item['Lease_Name'], item['Well_Type'], item['Comments'], item['Operator'],
            item['Well_Name'], item['Well_Status'], item['API'], item['Well_No'], item['District'],
            item['API_Well_Number'], item['County'], item['Field'], item['Well_Type_'], item['Ground_Elevation'],
            item['Latitude'], item['Status_Date'], item['Footage_N_S'], item['Township'], item['TownshipDir'],
            item['Permit'], item['Zone_Pool'], item['Total_Depth'], item['Well_Status_'], item['Plugback_Total_Depth'],
            item['Longitude'], item['Section'], item['Footage_E_W'], item['Range'], item['RangeDir']))
        self.conn.commit()

    def store_database_history(self, item):
        self.cursor.execute("""INSERT INTO AR_History
                VALUES (?,?,?,?)""", (
            item['h_api_well_no'], item['h_type_work'], item['h_effective_date'], item['h_comments']))
        self.conn.commit()

    def store_database_perforation(self, item):
        self.cursor.execute("""INSERT INTO AR_Perforation
                VALUES (?,?,?,?,?,?,?,?,?,?)""", (
            item['P_API'], item['P_Permit'], item['P_Top'], item['P_Bottom'], item['P_Date_Perf'],
            item['P_Size'], item['P_Num_Shots'], item['P_Spacing'], item['P_Pool_Name'], item['P_Comments']))
        self.conn.commit()

    def store_database_stimulation(self, item):
        self.cursor.execute("""INSERT INTO AR_Stimulation
               VALUES (?,?,?,?,?,?,?,?,?,?)""", (
            item['S_API'], item['S_Permit'], item['S_Date_Treated'], item['S_Top'], item['S_Bottom'],
            item['S_Hole_Type'], item['S_Fluid_Type'], item['S_Fluid_Amount'], item['S_Fluid_Unit'], item['S_Comment']))
        self.conn.commit()

    def store_database_production(self, item):
        self.cursor.execute("""INSERT INTO AR_Production 
        VALUES (?,?,?,?,?,?,?,?)""", (
            item['PR_API'], item['PR_PRU_ID'], item['PR_Report_Date'], item['PR_Lease_PRU_Name'], item['PR_Oil_Bbls'],
            item['PR_Gas_Prod_MCF'], item['PR_Water_Bbls'], item['PR_Gas_Sales_MCF']))
        self.conn.commit()

    def store_database_injection(self, item):
        self.cursor.execute("""INSERT INTO AR_Injection
        VALUES (?,?,?,?,?,?,?,?,?)""", (
            item['I_API'], item['I_UIC_Permit'], item['I_Report_Date'], item['I_Inj_Days'], item['I_Vol_Liquid'],
            item['I_Vol_Gas'], item['I_Max_Inj_Rate'], item['I_Avg_Tbg_press'], item['I_Max_Tbg_Press']))
        self.conn.commit()

    def store_database_inspection(self, item):
        self.cursor.execute("""INSERT INTO AR_Inspection
                VALUES (?,?,?,?)""", (
            item['IN_API'], item['IN_Permit'], item['IN_Form'], item['IN_Inspect_Date']))
        self.conn.commit()
