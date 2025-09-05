import requests
import base64
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape  # added for XML safety
import time
starttime=time.time()
url = "https://etar-dev53.ds-fa.oraclepdemos.com:443/xmlpserver/services/ExternalReportWSSService"
username = "lisa.jones"
password = "FP8?H3y%"

# New parameter value (edit as needed)
P_CONTROL_BUDGET_NAME_VALUE = "FY14 Annual Control"

escaped_param = escape(P_CONTROL_BUDGET_NAME_VALUE)

soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"
                 xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
   <soap12:Header/>
   <soap12:Body>
      <pub:runReport>
         <pub:reportRequest>
            <pub:reportAbsolutePath>/Another Query/Test Query 1.xdo</pub:reportAbsolutePath>
            <pub:attributeFormat>xlsx</pub:attributeFormat>
            <pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload>
            <pub:parameterNameValues>
               <pub:item>
                  <pub:name>P_CONTROL_BUDGET_NAME</pub:name>
                  <pub:values>
                     <pub:item>{escaped_param}</pub:item>
                  </pub:values>
               </pub:item>
            </pub:parameterNameValues>
         </pub:reportRequest>
      </pub:runReport>
   </soap12:Body>
</soap12:Envelope>
"""

headers = {
    "Content-Type": "application/soap+xml;charset=UTF-8"
}

response = requests.post(url, data=soap_body, headers=headers, auth=(username, password))

if response.status_code == 200:
    ns = {
        "soap12": "http://www.w3.org/2003/05/soap-envelope",
        "pub": "http://xmlns.oracle.com/oxp/service/PublicReportService"
    }
    root = ET.fromstring(response.text)
    report_bytes_element = root.find(".//pub:reportBytes", ns)
    if report_bytes_element is not None and report_bytes_element.text:
        excel_data = base64.b64decode(report_bytes_element.text)
        with open("report.xlsx", "wb") as f:
            f.write(excel_data)
        print("✅ Report saved as report.xlsx")
    else:
        print("❌ No <reportBytes> found in response")
        print(response.text)
else:
    print(f"❌ HTTP Error {response.status_code}")
    print(response.text)
endtime=time.time()
print(f"⏱️  Elapsed time: {endtime - starttime:.2f} seconds")
