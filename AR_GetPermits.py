from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import pyodbc

conn = pyodbc.connect('your_pyodbc_con_string')
cursor = conn.cursor()
file_suffix = 'test'
start = datetime.now()
print("PyScript started on:", start)
chrome_option = webdriver.ChromeOptions()
chrome_option.add_argument("incognito")
chrome_option.add_argument("headless")
gc = webdriver.Chrome(executable_path="your_webdriver_path", options=chrome_option)
gc.get("http://www.aogc.state.ar.us/welldata/wells.aspx")
WebDriverWait(gc, 60).until(EC.url_contains('welldata'))
gc.find_element(By.XPATH, "//input[@value='Permit']").click()
gc.find_element(By.XPATH, "//input[@id='cpMainContent_ChildContent2_btnGo']").click()
WebDriverWait(gc, 60).until(
    EC.element_to_be_clickable((By.XPATH, "//a[@id='cpMainContent_ChildContent2_lkCnt']"))).click()
elements = "//div[@id='cpMainContent_ChildContent2_GridID']//table//tbody//tr//td//a"
drop_table = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'AR_Permits_" + file_suffix + \
             "' AND TABLE_SCHEMA = 'dbo') DROP TABLE dbo.AR_Permits_" + file_suffix + ";"
create_table = "CREATE TABLE AR_Permits_" + file_suffix + \
               "(SNo int identity,Permit_Number NVARCHAR (50),[Source] NVARCHAR (500)," \
               "[TimeStamp] DATETIME NOT NULL DEFAULT(GETDATE()))"
cursor.execute(drop_table)
cursor.execute(create_table)
WebDriverWait(gc, 180).until(EC.presence_of_element_located((By.XPATH, "(" + elements + ")[1]")))
html = gc.find_element(By.XPATH, "//div[@id='cpMainContent_ChildContent2_GridID']")
html_data = html.get_attribute('outerHTML')
html_data = BeautifulSoup(html_data, 'html.parser')
all_element = html_data.findAll("tr")
print("Total Elems:", len(all_element))
all_list = []
a = 0


for element in all_element:
    all_list.append(element.text.strip())
df = pd.DataFrame(all_list)
for permit in all_list:
    cursor.execute(
        "INSERT INTO AR_Permits_" + file_suffix +
        "([Permit_Number],[Source]) VALUES (?,?)", permit.split(" ")[0], permit)
conn.commit()
print('Table created:', 'US_Wells_DataExtract_OLD.dbo.AR_Permits_' + file_suffix)
end = datetime.now()
cursor.close()
print('PyScript ended on:', end)
print('Time Taken:', end - start)

