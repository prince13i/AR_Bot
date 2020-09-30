import scrapy
import pyodbc
import re
from scrapy.http.request import Request
from scrapy.utils.response import open_in_browser
from ..items import ArAutomationItem
from ..items import ArAutomationItemHistory
from ..items import ArAutomationItemPerforation
from ..items import ArAutomationItemStimulation
from ..items import ArAutomationItemProduction
from ..items import ArAutomationItemInjection
from ..items import ArAutomationItemInspection

data_query = "SELECT * FROM AR_Permits_test"


class PermitsSpider(scrapy.Spider):
    name = 'permits'

    def start_requests(self):
        conn = pyodbc.connect('your_pyodbc_con_string')
        cursor = conn.cursor()
        cursor.execute(data_query)
        permit_number = [data[1] for data in cursor.fetchall()]
        for permit in permit_number:
            if permit:
                yield Request('http://www.aogc.state.ar.us/welldata/Pages/Permits.aspx?Permit=' + str(permit),
                              self.parse)

    def parse(self, response):
        item_instance = ArAutomationItem()
        his_item_instance = ArAutomationItemHistory()
        perf_item_instance = ArAutomationItemPerforation()
        stim_item_instance = ArAutomationItemStimulation()
        prod_item_instance = ArAutomationItemProduction()
        inj_item_instance = ArAutomationItemInjection()
        ins_item_instance = ArAutomationItemInspection()

        permit_number = response.xpath('//span[@id="cpMainContent_tblPermit"]/text()').get()
        lease_name = response.xpath('//span[@id="cpMainContent_tblLease"]/text()').get()
        well_type = response.xpath('//span[@id="cpMainContent_tblWellType"]/text()').get()
        comments = response.xpath('//span[@id="cpMainContent_tblWellComments"]/text()').get()
        operator = response.xpath('//*[@id="cpMainContent_tblCompany"]//text()').get()
        well_name = response.xpath('//span[@id="cpMainContent_tblWellName"]/text()').get()
        well_status = response.xpath('//span[@id="cpMainContent_tblWellStatus"]/text()').get()
        api = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').get()
        well_no = response.xpath('//span[@id="cpMainContent_tblWellNo"]/text()').get()
        district = response.xpath('//span[@id="cpMainContent_tblDistrict"]/text()').get()
        api_well_number = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblAPI_0"]/text()').get()
        county = response.xpath('//*[@id="cpMainContent_Container1_tabData_rptWellData_lblCounty_0"]//text()').get()
        field = response.xpath('//*[@id="cpMainContent_Container1_tabData_rptWellData_lblField_0"]//text()').get()
        well_type_ = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblWellType_0"]/text()').get()
        ground_elevation = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblGrEl_0"]/text()').get()
        latitude = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_lblLat_0"]/text()').get()
        status_date = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblPhone_0"]/text()').get()
        footage_n_s = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblType_0"]/text()').get()
        township = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_lblTwn_0"]/text()').get()
        twn_dir = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_Label5_0"]/text()').get()
        permit = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_lblPermit_0"]/text()').get()
        zone_pool = response.xpath('//*[@id="cpMainContent_Container1_tabData_rptWellData_lblPool_0"]//text()').get()
        total_depth = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_Label3_0"]/text()').get()
        well_status_ = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblWellStatus_0"]/text()').get()
        plugback_total_depth = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_lblTD_0"]/text()').get()
        longitude = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_lblLong_0"]/text()').get()
        section = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_lblSection_0"]/text()').get()
        footage_e_w = response.xpath(
            '//span[@id="cpMainContent_Container1_tabData_rptWellData_Label10_0"]/text()').get()
        range_ = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_Label15_0"]/text()').get()
        range_dir = response.xpath('//span[@id="cpMainContent_Container1_tabData_rptWellData_Label16_0"]/text()').get()
        item_instance['Permit_Number'] = permit_number
        item_instance['Lease_Name'] = lease_name
        item_instance['Well_Type'] = well_type
        item_instance['Comments'] = re.sub(' +', ' ', comments)
        item_instance['Operator'] = operator
        item_instance['Well_Name'] = well_name
        item_instance['Well_Status'] = well_status
        item_instance['API'] = api
        item_instance['Well_No'] = well_no
        item_instance['District'] = district
        item_instance['API_Well_Number'] = api_well_number
        item_instance['County'] = county
        item_instance['Field'] = field
        item_instance['Well_Type_'] = well_type_
        item_instance['Ground_Elevation'] = ground_elevation
        item_instance['Latitude'] = latitude
        item_instance['Status_Date'] = status_date
        item_instance['Footage_N_S'] = footage_n_s
        item_instance['Township'] = township
        item_instance['TownshipDir'] = twn_dir
        item_instance['Permit'] = permit
        item_instance['Zone_Pool'] = zone_pool
        item_instance['Total_Depth'] = total_depth
        item_instance['Well_Status_'] = well_status_
        item_instance['Plugback_Total_Depth'] = plugback_total_depth
        item_instance['Longitude'] = longitude
        item_instance['Section'] = section
        item_instance['Footage_E_W'] = footage_e_w
        item_instance['Range'] = range_
        item_instance['RangeDir'] = range_dir
        yield item_instance

        history_table = response.xpath('//*[@id="cpMainContent_Container1_tabHistory_GridHistory"]//tr')
        if history_table:
            for row in history_table[1:]:
                his_item_instance['h_api_well_no'] = row.xpath('td[1]//text()').extract_first()
                his_item_instance['h_type_work'] = row.xpath('td[2]//text()').extract_first()
                his_item_instance['h_effective_date'] = row.xpath('td[3]//text()').extract_first()
                his_item_instance['h_comments'] = row.xpath('td[4]//text()').extract_first()
                yield his_item_instance
        else:
            his_item_instance['h_api_well_no'] = response.\
                xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
            his_item_instance['h_type_work'] = None
            his_item_instance['h_effective_date'] = None
            his_item_instance['h_comments'] = None
            yield his_item_instance

        perforation_table = response.xpath('//*[@id="cpMainContent_Container1_tabPerfs_GridPerfs"]//tr')
        if perforation_table:
            for row in perforation_table[1:]:
                perf_item_instance['P_API'] = response.\
                    xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
                perf_item_instance['P_Permit'] = row.xpath('td[1]//text()').extract_first()
                perf_item_instance['P_Top'] = row.xpath('td[2]//text()').extract_first()
                perf_item_instance['P_Bottom'] = row.xpath('td[3]//text()').extract_first()
                perf_item_instance['P_Date_Perf'] = row.xpath('td[4]//text()').extract_first()
                perf_item_instance['P_Size'] = row.xpath('td[5]//text()').extract_first()
                perf_item_instance['P_Num_Shots'] = row.xpath('td[6]//text()').extract_first()
                perf_item_instance['P_Spacing'] = row.xpath('td[7]//text()').extract_first()
                perf_item_instance['P_Pool_Name'] = row.xpath('td[8]//text()').extract_first()
                perf_item_instance['P_Comments'] = row.xpath('td[9]//text()').extract_first()
                yield perf_item_instance
        else:
            perf_item_instance['P_API'] = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
            perf_item_instance['P_Permit'] = None
            perf_item_instance['P_Top'] = None
            perf_item_instance['P_Bottom'] = None
            perf_item_instance['P_Date_Perf'] = None
            perf_item_instance['P_Size'] = None
            perf_item_instance['P_Num_Shots'] = None
            perf_item_instance['P_Spacing'] = None
            perf_item_instance['P_Pool_Name'] = None
            perf_item_instance['P_Comments'] = None
            yield perf_item_instance

        stimulation_table = response.xpath('//*[@id="cpMainContent_Container1_tabStim_GridStim"]//tr')
        if stimulation_table:
            for row in stimulation_table[1:]:
                stim_item_instance['S_API'] = response.\
                    xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
                stim_item_instance['S_Permit'] = row.xpath('td[1]//text()').extract_first()
                stim_item_instance['S_Date_Treated'] = row.xpath('td[2]//text()').extract_first()
                stim_item_instance['S_Top'] = row.xpath('td[3]//text()').extract_first()
                stim_item_instance['S_Bottom'] = row.xpath('td[4]//text()').extract_first()
                stim_item_instance['S_Hole_Type'] = row.xpath('td[5]//text()').extract_first()
                stim_item_instance['S_Fluid_Type'] = row.xpath('td[6]//text()').extract_first()
                stim_item_instance['S_Fluid_Amount'] = row.xpath('td[7]//text()').extract_first()
                stim_item_instance['S_Fluid_Unit'] = row.xpath('td[8]//text()').extract_first()
                stim_item_instance['S_Comment'] = row.xpath('td[9]//text()').extract_first()
                yield stim_item_instance
        else:
            stim_item_instance['S_API'] = response.\
                xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
            stim_item_instance['S_Permit'] = None
            stim_item_instance['S_Date_Treated'] = None
            stim_item_instance['S_Top'] = None
            stim_item_instance['S_Bottom'] = None
            stim_item_instance['S_Hole_Type'] = None
            stim_item_instance['S_Fluid_Type'] = None
            stim_item_instance['S_Fluid_Amount'] = None
            stim_item_instance['S_Fluid_Unit'] = None
            stim_item_instance['S_Comment'] = None
            yield stim_item_instance

        production_table = response.xpath('//*[@id="cpMainContent_Container1_tabProd_GridProd"]//tr')
        if production_table:
            for row in production_table[1:-1]:
                pr_api = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
                prod_item_instance['PR_API'] = pr_api
                prod_item_instance['PR_PRU_ID'] = row.xpath('td[1]//text()').extract_first()
                prod_item_instance['PR_Report_Date'] = row.xpath('td[2]//text()').extract_first()
                prod_item_instance['PR_Lease_PRU_Name'] = row.xpath('td[3]//span//text()').extract_first()
                prod_item_instance['PR_Oil_Bbls'] = row.xpath('td[4]//span//text()').extract_first()
                prod_item_instance['PR_Gas_Prod_MCF'] = row.xpath('td[5]//span//text()').extract_first()
                prod_item_instance['PR_Water_Bbls'] = row.xpath('td[6]//span//text()').extract_first()
                prod_item_instance['PR_Gas_Sales_MCF'] = row.xpath('td[7]//span//text()').extract_first()
                yield prod_item_instance
        else:
            pr_api = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
            prod_item_instance['PR_API'] = pr_api
            prod_item_instance['PR_PRU_ID'] = None
            prod_item_instance['PR_Report_Date'] = None
            prod_item_instance['PR_Lease_PRU_Name'] = None
            prod_item_instance['PR_Oil_Bbls'] = None
            prod_item_instance['PR_Gas_Prod_MCF'] = None
            prod_item_instance['PR_Water_Bbls'] = None
            prod_item_instance['PR_Gas_Sales_MCF'] = None
            yield prod_item_instance

        injection_table = response.xpath('//*[@id="cpMainContent_Container1_tabIMIT_GridIMIT"]//tr')
        if injection_table:
            for row in injection_table[1:]:
                inj_item_instance['I_API'] = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').get()
                inj_item_instance['I_UIC_Permit'] = row.xpath('td[1]//text()').extract_first()
                inj_item_instance['I_Report_Date'] = row.xpath('td[2]//text()').extract_first()
                inj_item_instance['I_Inj_Days'] = row.xpath('td[3]//text()').extract_first()
                inj_item_instance['I_Vol_Liquid'] = row.xpath('td[4]//text()').extract_first()
                inj_item_instance['I_Vol_Gas'] = row.xpath('td[5]//text()').extract_first()
                inj_item_instance['I_Max_Inj_Rate'] = row.xpath('td[6]//text()').extract_first()
                inj_item_instance['I_Avg_Tbg_press'] = row.xpath('td[7]//text()').extract_first()
                inj_item_instance['I_Max_Tbg_Press'] = row.xpath('td[8]//text()').extract_first()
                yield inj_item_instance
        else:
            inj_item_instance['I_API'] = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').get()
            inj_item_instance['I_UIC_Permit'] = None
            inj_item_instance['I_Report_Date'] = None
            inj_item_instance['I_Inj_Days'] = None
            inj_item_instance['I_Vol_Liquid'] = None
            inj_item_instance['I_Vol_Gas'] = None
            inj_item_instance['I_Max_Inj_Rate'] = None
            inj_item_instance['I_Avg_Tbg_press'] = None
            inj_item_instance['I_Max_Tbg_Press'] = None
            yield inj_item_instance

        inspection_table = response.xpath('//*[@id="cpMainContent_Container1_tabInspect_GridInspect"]//tr')
        if inspection_table:
            for row in inspection_table[1:]:
                ins_item_instance['IN_API'] = response.\
                    xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
                ins_item_instance['IN_Permit'] = row.xpath('td[1]//span//text()').extract_first()
                ins_item_instance['IN_Form'] = row.xpath('td[2]//span//text()').extract_first()
                ins_item_instance['IN_Inspect_Date'] = row.xpath('td[3]//a//text()').extract_first()
                yield ins_item_instance
        else:
            ins_item_instance['IN_API'] = response.xpath('//span[@id="cpMainContent_tblAPI"]/text()').extract_first()
            ins_item_instance['IN_Permit'] = None
            ins_item_instance['IN_Form'] = None
            ins_item_instance['IN_Inspect_Date'] = None
            yield ins_item_instance
