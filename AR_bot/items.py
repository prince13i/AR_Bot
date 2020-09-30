import scrapy


class ArAutomationItem(scrapy.Item):
    Permit_Number = scrapy.Field()
    Lease_Name = scrapy.Field()
    Well_Type = scrapy.Field()
    Comments = scrapy.Field()
    Operator = scrapy.Field()
    Well_Name = scrapy.Field()
    Well_Status = scrapy.Field()
    API = scrapy.Field()
    Well_No = scrapy.Field()
    District = scrapy.Field()
    API_Well_Number = scrapy.Field()
    County = scrapy.Field()
    Field = scrapy.Field()
    Well_Type_ = scrapy.Field()
    Ground_Elevation = scrapy.Field()
    Latitude = scrapy.Field()
    Status_Date = scrapy.Field()
    Footage_N_S = scrapy.Field()
    Township = scrapy.Field()
    TownshipDir = scrapy.Field()
    Permit = scrapy.Field()
    Zone_Pool = scrapy.Field()
    Total_Depth = scrapy.Field()
    Well_Status_ = scrapy.Field()
    Plugback_Total_Depth = scrapy.Field()
    Longitude = scrapy.Field()
    Section = scrapy.Field()
    Footage_E_W = scrapy.Field()
    Range = scrapy.Field()
    RangeDir = scrapy.Field()


class ArAutomationItemHistory(scrapy.Item):
    h_api_well_no = scrapy.Field()
    h_type_work = scrapy.Field()
    h_effective_date = scrapy.Field()
    h_comments = scrapy.Field()


class ArAutomationItemPerforation(scrapy.Item):
    P_API = scrapy.Field()
    P_Permit = scrapy.Field()
    P_Top = scrapy.Field()
    P_Bottom = scrapy.Field()
    P_Date_Perf = scrapy.Field()
    P_Size = scrapy.Field()
    P_Num_Shots = scrapy.Field()
    P_Spacing = scrapy.Field()
    P_Pool_Name = scrapy.Field()
    P_Comments = scrapy.Field()


class ArAutomationItemStimulation(scrapy.Item):
    S_API = scrapy.Field()
    S_Permit = scrapy.Field()
    S_Date_Treated = scrapy.Field()
    S_Top = scrapy.Field()
    S_Bottom = scrapy.Field()
    S_Hole_Type = scrapy.Field()
    S_Fluid_Type = scrapy.Field()
    S_Fluid_Amount = scrapy.Field()
    S_Fluid_Unit = scrapy.Field()
    S_Comment = scrapy.Field()


class ArAutomationItemProduction(scrapy.Item):
    PR_API = scrapy.Field()
    PR_PRU_ID = scrapy.Field()
    PR_Report_Date = scrapy.Field()
    PR_Lease_PRU_Name = scrapy.Field()
    PR_Oil_Bbls = scrapy.Field()
    PR_Gas_Prod_MCF = scrapy.Field()
    PR_Water_Bbls = scrapy.Field()
    PR_Gas_Sales_MCF = scrapy.Field()


class ArAutomationItemInjection(scrapy.Item):
    I_API = scrapy.Field()
    I_UIC_Permit = scrapy.Field()
    I_Report_Date = scrapy.Field()
    I_Inj_Days = scrapy.Field()
    I_Vol_Liquid = scrapy.Field()
    I_Vol_Gas = scrapy.Field()
    I_Max_Inj_Rate = scrapy.Field()
    I_Avg_Tbg_press = scrapy.Field()
    I_Max_Tbg_Press = scrapy.Field()


class ArAutomationItemInspection(scrapy.Item):
    IN_API = scrapy.Field()
    IN_Permit = scrapy.Field()
    IN_Form = scrapy.Field()
    IN_Inspect_Date = scrapy.Field()
