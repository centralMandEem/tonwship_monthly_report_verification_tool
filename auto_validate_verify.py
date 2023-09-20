#@title Before running this part of code. Make sure that the 2 files opened are the report file to be checked and the file to record the verification/validation findings { display-mode: "form" }
import pandas as pd
import json
from datetime import datetime, timedelta
import gspread
from pandas.core.arrays.string_ import FloatingDtype
# Define functions
# Function to convert column alphabet to column index
def col_to_num(col_str):
    """ Convert base26 column string to number. """
    expn = 0
    col_num = 0
    for char in reversed(col_str):
        col_num += (ord(char) - ord('A') + 1) * (26 ** expn)
        expn += 1
    return col_num

# Function to convert string to number
def convert2float(data):
    try:
        newD = float(str(data).replace(",", ""))
    except:
        newD = 0
    # print("Converting " + str(data) + " | new data = " + str(newD))
    return newD

# Checking All_villages sheet
def checkAllVillagesSheet(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  allVillData = data['All_villages']['data']
  allProviderData = data['All_provider']['data']
  dataStartRow = int(data['All_villages']['headerRow']) + 1
  # print(json.dumps(allVillData, indent=4))
  allVillCheck = []
  if len(allVillData) > 0:
    for row, vc in enumerate(allVillData, start=dataStartRow):
      org = allVillData[vc]["Organization"]
      sr = allVillData[vc]["State_Region"]
      tsp = allVillData[vc]["Township"]
      rhc = allVillData[vc]["RHC_Name"]
      sc = allVillData[vc]["Sub-center_Name"]
      vname = allVillData[vc]["Name_of_Village"]
      vnamemm = allVillData[vc]["Name_of_Village_(Myanmar3)"]
      vow = allVillData[vc]["Village/Worksite"]
      hh = convert2float(allVillData[vc]["Covered_HH"])
      lpop = convert2float(allVillData[vc]["Covered_Pop_(Local_Resident_Only)"])
      migrant = convert2float(allVillData[vc]["Estimated_Migrants_POP"])
      tpop = convert2float(allVillData[vc]["Total_Population"])
      lat = convert2float(allVillData[vc]["Latitude"])
      lng = convert2float(allVillData[vc]["Longitude"])
      provider = allVillData[vc]["VMW/PP_(Y/N)"]
      casemx = allVillData[vc]["Case_Mx_(Y/N)"]
      llinDist = allVillData[vc]["LLIN_Dist._(Y/N)"]
      pse = allVillData[vc]["PSE_(Y/N)"]
      mimu = allVillData[vc]["MIMU_Pcode"]
      mpt = allVillData[vc]["MPT"]
      atom = allVillData[vc]["TELENOR"]
      ooredoo = allVillData[vc]["OOREDOO"]
      mytel = allVillData[vc]["MYTEL"]
      c450 = allVillData[vc]["CDMA450MZ"]
      c800 = allVillData[vc]["CDMA800MZ"]
      oNetwork = allVillData[vc]["Overall_network_coverage"]
      remark = allVillData[vc]["Remark"]
      addedDate = allVillData[vc]["Newly_added_date (starting from 24 Aug 2017)"]
      lastChangeDate = allVillData[vc]["Date_of_last_changes_made"]
      removedDate = allVillData[vc]["Removed_date"]
      status = allVillData[vc]["Village/WS_status"]
      removedReason = allVillData[vc]["Reason_for_removal"]
      # print(vc + ", " + str(hh) + "," + str(tpop) + "," + str(lat) + "," + str(lng))
      if removedDate != '' and removedReason == '':
        villageCheckString = f'row - {row} | Removed reason must be mentioned ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
        
      if removedDate == '' and removedReason != '' and status != 'Removed':
        villageCheckString = f'row - {row} | Village/worksite is under implementation but removed reason is mentioned ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
        
      if removedDate == '' and removedReason != '' and status == 'Removed':
        villageCheckString = f'row - {row} | Village/worksite is removed from project area but removed date is not mentioned ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
              
      if removedDate != '' and status != 'Removed':
        villageCheckString = f'row - {row} | Village was removed on {removedDate}. Status must be "Removed". ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
      
      if removedDate == '' and (org == '' or sr == '' or tsp == '' or rhc == '' or sc == '' or vname == '' or vnamemm == '' or vow == '' or provider == '' or casemx == '' or llinDist == '' or pse == '' or \
                                mpt == '' or atom == '' or ooredoo == '' or mytel == '' or c450 == '' or c800 == '' or oNetwork == '' or addedDate == '' or lastChangeDate == '' or status == ''):
        villageCheckString = f'row - {row} | Incomplete data ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
      
      if removedDate == '' and hh == 0:
        villageCheckString = f'row - {row} | HH data of EM coverage village/worksite must not be unknown and must be at least 1. ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
      
      if removedDate == '' and lpop + migrant != tpop:
        villageCheckString = f'row - {row} | Calculation of total population error. ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
      
      if removedDate == '' and tpop == 0:
        villageCheckString = f'row - {row} | Population data of EM coverage village/worksite must not be unknown and must be at least 1. ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])

      if removedDate == '' and (lat == 0 or lat > 50 or lng == 50 or lng == 0 or lng < 50):
        villageCheckString = f'row - {row} | GPS data required or GPS data needs to be rechecked. ({vc})'
        allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])
      
      if removedDate == '':
        checkProvider = "N"
        postCodes = list(allProviderData.keys())
        for postCode in allProviderData:
          for personCode in allProviderData[postCode]:
            if allProviderData[postCode][personCode]['Removed_date'] == '' and allProviderData[postCode][personCode]['Assigned_village_code'] == vc:
              checkProvider = "Y"
              break        
        if checkProvider != provider:
          villageCheckString = f'row - {row} | VMW/PP_(Y/N) is not consistent with All_provider sheet. ({vc})'
          allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet', villageCheckString])

    if len(allVillCheck) == 0:
      allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet','OK'])
  else:
    allVillCheck.append([mainOrg, mainSr, mainTsp, 'All_villages sheet','No data'])
  return allVillCheck

# Checking All_provider sheet
def checkAllProviderSheet(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  allProviderData = data['All_provider']['data']
  dataStartRow = int(data['All_provider']['headerRow']) + 1
  allProviderCheck = []
  if len(allProviderData) > 0:
    for providerPost in allProviderData:
      activeProvider = 0
      for person in allProviderData[providerPost]:
        removedDate = allProviderData[providerPost][person]['Removed_date']
        if removedDate == '':
          activeProvider += 1
        
        if removedDate !="" and allProviderData[providerPost][person]['Reason_for_removal'] == '':
          providerCheckString = f'Provider removed without mentioning the reason ({providerPost} | {person})'
          allProviderCheck.append([mainOrg, mainSr, mainTsp, 'All_provider sheet', providerCheckString])
        
        if removedDate =="" and allProviderData[providerPost][person]['Reason_for_removal'] != '':
          providerCheckString = f'Provider is active but reason for removel is mentioned. ({providerPost} | {person})'
          allProviderCheck.append([mainOrg, mainSr, mainTsp, 'All_provider sheet', providerCheckString])
            
        if removedDate == "" and allProviderData[providerPost][person]['Type_of_provider'] != "GP" and allProviderData[providerPost][person]['Type_of_provider'] != "Private hospital" and allProviderData[providerPost][person]['Type_of_provider'] != "PMI-EM Township level team" and \
          (allProviderData[providerPost][person]['Township'] == '' or  allProviderData[providerPost][person]['Type_of_provider'] == '' or \
            allProviderData[providerPost][person]['Included_in_PMI_indicator_(Y/N)'] == '' or  allProviderData[providerPost][person]['ICMV_(Y/N)'] == '' or  allProviderData[providerPost][person]['Name_Of_Provider'] == '' or \
            allProviderData[providerPost][person]['Sex'] == '' or allProviderData[providerPost][person]['Assigned_village_code'] == '' or  allProviderData[providerPost][person]['Assigned_village_name'] == '' or  \
            allProviderData[providerPost][person]['Newly_added_date (starting from 29 Jan 2018)'] == '' or allProviderData[providerPost][person]['Date_of_last_changes_made'] == ''):
          providerCheckString = f'Incomplete data ({providerPost} | {person})'
          allProviderCheck.append([mainOrg, mainSr, mainTsp, 'All_provider sheet', providerCheckString])

      if activeProvider > 1:
        providerCheckString = f'There are more than 1 active provider for the same provider post code ({providerPost} | {person})'
        allProviderCheck.append([mainOrg, mainSr, mainTsp, 'All_provider sheet', providerCheckString])
    if len(allProviderCheck) == 0:
      allProviderCheck.append([mainOrg, mainSr, mainTsp, 'All_provider sheet','OK'])
  else:
    allProviderCheck.append([mainOrg, mainSr, mainTsp, 'All_provider sheet','No data'])
  return allProviderCheck

# checking reporting period
def checkRpPeriod(verifyFindingSheet, mainOrg, mainSr, mainTsp, shName, row, rpMth, rpYr):
  rpYr = convert2float(rpYr)
  row = row
  rpMth = rpMth
  rpPeriodCheck =[]
  if rpYr > 0 and rpYr == 2023 and (rpMth == 'October' or rpMth == 'November' or rpMth == 'December'):
    checkStr = f"row - {row} | Future reporting period found ({rpMth} {rpYr})"
    rpPeriodCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])
  elif rpYr > 0 and rpYr == 2022 and rpMth != 'October' and rpMth != 'November' and rpMth != 'December':
    checkStr = f"row - {row} | Reporting period of previous fiscal year found ({rpMth} {rpYr})"
    rpPeriodCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])
  return rpPeriodCheck

# checking village code, rhc, sc, village name
def checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, shName, row, vc, rhc, sc, vname, vow=None):
  vilData = data['All_villages']['data']
  vcCheck = []
  if vc!= '' and len(vc) != 12:
    checkStr = f"row - {row} | Village code format error"
    vcCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])
  if vow==None:
    if vc != '' and vc[-4:] != '9999' and vc[-4:] != '9998' and len(vc) == 12 and (rhc != vilData[vc]['RHC_Name'] or sc != vilData[vc]['Sub-center_Name'] or vname != vilData[vc]['Name_of_Village']):
      checkStr = f"row - {row} | Village code, RHC, Subcenter or village name is not exactly the same as those mentioned in All_villages sheet"
      vcCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])
  else:
    if vc != '' and vc[-4:] != '9999' and vc[-4:] != '9998' and len(vc) == 12 and (rhc != vilData[vc]['RHC_Name'] or sc != vilData[vc]['Sub-center_Name'] or vname != vilData[vc]['Name_of_Village'] or vow != vilData[vc]['Village/Worksite']):
      checkStr = f"row - {row} | Village code, RHC, Subcenter, village name or Village/Worksite is not exactly the same as those mentioned in All_villages sheet"
      vcCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])
  return vcCheck

# checking personCode and type of provider
def checkProviderType(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, shName, row, personCode, providerType):
  providerData = data['All_provider']['data']
  providerTypeCheck = []
  if personCode != '' and len(personCode) != 9:
    checkStr = f"row - {row} | Person code format error"
    providerTypeCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])

  if personCode != '' and len(personCode) == 9 and providerData[personCode[:-2]][personCode]['Type_of_provider'] != providerType:
    checkStr = f"row - {row} | Provider code and provider type is not exactly the same as those mentioned in All_provider sheet"
    providerTypeCheck.append([mainOrg, mainSr, mainTsp, shName, checkStr])
  return providerTypeCheck

# checking Potential malaria outbreak sheet
def checkPMO(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):  
  pmoData = data["'Potential malaria outbreak'"]['data']
  dataStartRow = int(data["'Potential malaria outbreak'"]['headerRow']) + 1
  pmoCheck = []
  if len(pmoData) > 0:
    for row, pmo in enumerate(pmoData, start=dataStartRow):
      org = pmo['Organization']
      sr = pmo['State/Region']
      tsp = pmo['Township']
      rpMth = pmo['Reporting month']
      rpYr = pmo['Reporting year']
      vc = pmo['Location code']
      rhc = pmo['RHC']
      sc = pmo['Subcenter']
      vname = pmo['Village name']
      threshold = convert2float(pmo['Threadshold'])
      curData = convert2float(pmo['Current data'])
      rp2THD = pmo['Reported to THD']
      rpDate = pmo['Reported date']
      obOccur = pmo['Outbreak occur']
      sDate = pmo['Start date']
      eDate = pmo['End date']

      

      if rp2THD != "Y" and rp2THD != "":
        checkStr = f"row - {row} | If not reported to THD, doesn't need to mention in this report"
        pmoCheck.append([mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', checkStr])

      if rp2THD == "Y" and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or vc == '' or rhc == '' or sc == '' or vname == '' or threshold == '' or curData == '' or curData < threshold or rpDate == '' or obOccur == ''):
        checkStr = f"row - {row} | Incomplete data"
        pmoCheck.append([mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', checkStr])

      if rp2THD == "Y" and obOccur == "Y" and sDate == '':
        checkStr = f"row - {row} | Incomplete data. Outbreak occur and start date not mentioned."
        pmoCheck.append([mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', checkStr])

      pmoCheck = pmoCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', row, rpMth, rpYr)
      pmoCheck = pmoCheck + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'Potential malaria outbreak sheet', row, vc, rhc, sc, vname)   

    if len(pmoCheck) == 0:
      pmoCheck.append([mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', 'OK'])
  else:
    checkStr = "No data"
    pmoCheck.append([mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', checkStr])
  return pmoCheck

def checkPLA(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  vilData = data['All_villages']['data']
  plaData = data["'PLA session'"]['data']
  # print(json.dumps(plaData, indent=4))
  dataStartRow = int(data["'PLA session'"]['headerRow']) + 1
  plaCheck = []
  if len(plaData) > 0:
    for row, pla in enumerate(plaData, start=dataStartRow):
      org = pla['Organization']
      sr = pla['State/Region']
      tsp = pla['Township']
      rpMth = pla['Reporting month']
      rpYr = pla['Reporting year']
      date = pla['Date of activity']
      vc = pla['Location code']
      rhc = pla['RHC']
      sc = pla['Subcenter']
      vname = pla['Village name']
      facilitatorType = pla['Type of facilitator']
      map = pla['Mapping']
      sia = pla['Situation Analysis']
      aua = pla['Audiance analysis']
      sta = pla['Seasonal Trend Analysis']
      pta = pla['Problem Tree analysis']
      trw = pla['Transect walk']
      tsm = pla['Ten Seeds method']
      fgd = pla['Focused group discussion']
      lpy = pla['La phet yay gyan wine']
      male = convert2float(pla['Male attendance'])
      female = convert2float(pla['Female attendance'])
      total = convert2float(pla['Total attendance'])
      migrant = convert2float(pla['# of migrants included'])

      if male + female != total:
        checkStr = f"row - {row} | Total attendance calculation error"
        plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', checkStr])

      if total > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or date == '' or vc == '' or rhc == '' or sc == '' or vname == '' or facilitatorType == '' or \
                        map == '' or sia == '' or aua == '' or sta == '' or pta == '' or trw == '' or tsm == '' or fgd == '' or lpy == ''):
        checkStr = f"row - {row} | Incomplete data"
        plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', checkStr])
      
      if total <= 0 and (date != '' or vc != '' or rhc != '' or sc != '' or vname != '' or facilitatorType != '' or \
                        map != '' or sia != '' or aua != '' or sta != '' or pta != '' or trw != '' or tsm != '' or fgd != '' or lpy != ''):
        checkStr = f"row - {row} | Activities with 0 or blank total attendance doesn't need to be mentioned in this report."
        plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', checkStr])

      if vc!= '' and len(vc) != 12:
        checkStr = f"row - {row} | Village code format error"
        plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', checkStr])

      if vc != '' and vc[-4:] != '9999' and vc[-4:] != '9998' and len(vc) == 12 and (rhc != vilData[vc]['RHC_Name'] or sc != vilData[vc]['Sub-center_Name'] or vname != vilData[vc]['Name_of_Village']):
        checkStr = f"row - {row} | Village code, RHC, Subcenter or village name is not exactly the same as those mentioned in All_villages sheet"
        plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', checkStr])

      plaCheck = plaCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'Potential malaria outbreak sheet', row, rpMth, rpYr)
      plaCheck = plaCheck + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'Potential malaria outbreak sheet', row, vc, rhc, sc, vname)    
      
    if len(plaCheck) == 0:
      plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', 'OK'])
  else:
    checkStr = "No data"
    plaCheck.append([mainOrg, mainSr, mainTsp, 'PLA session sheet', checkStr])
  return plaCheck

def checkIpcAdditional(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):  
  ipcData = data["IPC_additional"]['data']
  # print(json.dumps(ipcData, indent=4))
  dataStartRow = int(data["IPC_additional"]['headerRow']) + 1
  ipcCheck = []
  if len(ipcData) > 0:
    for row, ipc in enumerate(ipcData, start=dataStartRow):
      org = ipc['Organization']
      sr = ipc['State/Region']
      tsp = ipc['Township']
      rpMth = ipc['Reporting month']
      rpYr = ipc['Reporting year']
      personCode = ipc['Person code']
      providerType = ipc['Type of provider']
      male = convert2float(ipc['Male attendance'])
      female = convert2float(ipc['Female attendance'])
      total = convert2float(ipc['Total attendance'])
      migrant = convert2float(ipc['# of migrants included'])

      if male + female != total:
        checkStr = f"row - {row} | Total attendance calculation error"
        ipcCheck.append([mainOrg, mainSr, mainTsp, 'IPC_additional sheet', checkStr])
      
      if total > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or personCode == '' or providerType == ''):
        checkStr = f"row - {row} | Incomplete data"
        ipcCheck.append([mainOrg, mainSr, mainTsp, 'IPC_additional sheet', checkStr])
      
      if total <= 0 and (personCode != '' or providerType != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        ipcCheck.append([mainOrg, mainSr, mainTsp, 'IPC_additional sheet', checkStr])

      ipcCheck = ipcCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp,'IPC_additional sheet', row, rpMth, rpYr)
      ipcCheck = ipcCheck + checkProviderType(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'IPC_additional sheet', row, personCode, providerType)
      
    if len(ipcCheck) == 0:
      ipcCheck.append([mainOrg, mainSr, mainTsp, 'IPC_additional sheet', 'OK'])
  else:
    checkStr = "No data"
    ipcCheck.append([mainOrg, mainSr, mainTsp, 'IPC_additional sheet', checkStr])
  return ipcCheck

def checkGhtWsHe(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  ghtData = data["'GHT,Worksite HE'"]['data']
  # print(json.dumps(ghtData, indent=4))
  dataStartRow = int(data["'GHT,Worksite HE'"]['headerRow']) + 1
  ghtCheck = []
  if len(ghtData) > 0:
    for row, ght in enumerate(ghtData, start=dataStartRow):
      org = ght['Organization']
      sr = ght['State/Region']
      tsp = ght['Township']
      rpMth = ght['Reporting month']
      rpYr = ght['Reporting year']
      personCode = ght['Person code']
      providerType = ght['Type of provider']
      vc = ght['Location code']
      rhc = ght['RHC']
      sc = ght['Subcenter']
      vname = ght['Village name']
      vow = ght['Village/Worksite']
      heSess = convert2float(ght['# of HE sessions'])
      male = convert2float(ght['Male attendance'])
      female = convert2float(ght['Female attendance'])
      total = convert2float(ght['Total attendance'])
      migrant = convert2float(ght['# of migrants included'])

      if male + female != total:
        checkStr = f"row - {row} | Total attendance calculation error"
        ghtCheck.append([mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', checkStr])

      if (total >0 and heSess <= 0) or (total <=0 and heSess > 0):
        checkStr = f"row - {row} | Incorrect data"
        ghtCheck.append([mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', checkStr])

      if total > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or personCode == '' or providerType == '' or vc == '' or rhc == '' or sc == '' or vname == '' or vow == ''):
        checkStr = f"row - {row} | Incomplete data. (Only the data of project staff activity and activity of provider under EM project need to be mentioned in this report"
        ghtCheck.append([mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', checkStr])
      
      if total <=0 and (personCode != '' or providerType != '' or vc != '' or rhc != '' or sc != '' or vname != '' or vow != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        ghtCheck.append([mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', checkStr])

      ghtCheck = ghtCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', row, rpMth, rpYr)
      ghtCheck = ghtCheck + checkProviderType(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'GHT,Worksite HE sheet', row, personCode, providerType)
      ghtCheck = ghtCheck + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'GHT,Worksite HE sheet sheet', row, vc, rhc, sc, vname, vow)
      
    if len(ghtCheck) == 0:
      ghtCheck.append([mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', 'OK'])
  else:
    checkStr = "No data"
    ghtCheck.append([mainOrg, mainSr, mainTsp, 'GHT,Worksite HE sheet', checkStr])
  return ghtCheck

def checkLlinDistMassCont(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  llinData = data["'LLIN dist(mass,continuous)'"]['data']
  # print(json.dumps(llinData, indent=4))
  dataStartRow = int(data["'LLIN dist(mass,continuous)'"]['headerRow']) + 1
  llinCheck = []
  if len(llinData) > 0:
    for row, llin in enumerate(llinData, start=dataStartRow):
      org = llin["Organization"]
      sr = llin["State/Region"]
      tsp = llin["Township"]
      rpMth = llin["Reporting month"]
      rpYr = llin["Reporting year"]
      date = llin["Date"]
      vc = llin["Location code"]
      rhc = llin["RHC"]
      sc = llin["Subcenter"]
      vname = llin["Village name"]
      vow = llin["Village/Worksite"]
      wType = llin["Type\n(If worksite, mention the \"type of worksite\". If village, fill \"-\")"].replace("-","")
      wType = wType.replace("-","")
      hhPresent = llin["# of HH Present"]
      hhCovered = llin["# of HH covered by LLIN"]
      hhPercent = llin["% of HH Covered by LLIN"]
      popPresent = llin["# of Population Present"]
      popCovered = llin["# of Population Covered By LLIN"]
      u5POP = llin["# of Under 5 Population Covered By LLIN"]
      a5POP = llin["# of Above 5 Population Covered By LLIN"]
      popPercent = llin["% of Population Covered By LLIN"]
      preg = llin["Pregnant"]
      migrant = llin["# of Migrant Workers"]
      llinAmt = llin["Total # of LLIN Distributed"]
      popLlin = llin["Net Ownership (Population/LLIN)"]
      mc = llin["Mass/Continuous"]
      brand = llin["Brand Name"]
      remark = llin["Remark"]
      completeness = llin["Data completeness"]

      hhPresentNum = convert2float(llin["# of HH Present"])
      hhCoveredNum = convert2float(llin["# of HH covered by LLIN"])
      popPresentNum = convert2float(llin["# of Population Present"])
      popCoveredNum = convert2float(llin["# of Population Covered By LLIN"])
      u5POPNum = convert2float(llin["# of Under 5 Population Covered By LLIN"])
      a5POPNum = convert2float(llin["# of Above 5 Population Covered By LLIN"])
      pregNum = convert2float(llin["Pregnant"])
      migrantNum = convert2float(llin["# of Migrant Workers"])
      llinAmtNum = convert2float(llin["Total # of LLIN Distributed"])
      popLlinNum = convert2float(llin["Net Ownership (Population/LLIN)"])

      if llinAmtNum > 0 and (hhPresentNum < hhCoveredNum or a5POPNum + u5POPNum != popCoveredNum or popPresentNum < popCoveredNum or popCoveredNum < pregNum or popCoveredNum < migrantNum):
        checkStr = f"row - {row} | Data entry error in HH and POP data"
        llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', checkStr])

      if llinAmtNum > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or date == '' or vc == '' or rhc == '' or sc == '' or vname == '' or vow == '' or hhPresent == '' or hhCovered == '' or \
                             popPresent == '' or popCovered == '' or u5POP == '' or a5POP == '' or preg == '' or migrant == '' or llinAmt == '' or popLlin == '' or mc == '' or brand == '' or completeness == ''):
        checkStr = f"row - {row} | Incomplete data"
        llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', checkStr])

      if llinAmtNum <= 0 and (date != '' or vc != '' or rhc != '' or sc != '' or vname != '' or vow != '' or wType != '' or migrant != '' or mc != '' or brand != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', checkStr])

      if vow == 'Worksite' and wType == '':
        checkStr = f"row - {row} | Location is worksite but type of worksite is not mentioned"
        llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', checkStr])
      
      if vow == 'Village' and wType != '':
        checkStr = f"row - {row} | Location is not worksite but type of worksite is mentioned"
        llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', checkStr])

      llinCheck = llinCheck + checkRpPeriod(verifyFindingSheet, mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', row, rpMth, rpYr)
      llinCheck = llinCheck + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'LLIN dist(mass,continuous) sheet', row, vc, rhc, sc, vname, vow)
      
    if len(llinCheck) == 0:
      llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', 'OK'])
  else:
    checkStr = "No data"
    llinCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(mass,continuous) sheet', checkStr])
  return llinCheck

def checkLlinAnc(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  ancData = data["'LLIN dist(ANC)'"]['data']
  # print(json.dumps(ancData, indent=4))
  dataStartRow = int(data["'LLIN dist(ANC)'"]['headerRow']) + 1
  ancCheck = []
  if len(ancData) > 0:
    for row, anc in enumerate(ancData, start=dataStartRow):
      org = anc["Organization"]
      sr = anc["State/Region"]
      tsp = anc["Township"]
      rpMth = anc["Reporting month"]
      rpYr = anc["Reporting year"]
      rhc = anc["Name of RHC"]
      sc = anc["Name of Subcenter"]
      vname = anc["Village/Worksite name of patient"]
      hh = anc["Household present under Subcenter"]
      pop = anc["Population present under Subcenter"]
      distMth = anc["LLIN distributed month"]
      distYr = anc["LLIN distributed year"]
      pregAttend = anc["No. of pregnant women attending at least one time"]
      pregTest = anc["No. of pregnant women tested"]
      pregPos = anc["No. of pregnant women with malaria positive"]
      llinAmt = anc["No. of LLIN distributed to pregnant women"]
      
      pregAttendNum = convert2float(anc["No. of pregnant women attending at least one time"])
      pregTestNum = convert2float(anc["No. of pregnant women tested"])
      pregPosNum = convert2float(anc["No. of pregnant women with malaria positive"])
      llinAmtNum = convert2float(anc["No. of LLIN distributed to pregnant women"])

      if llinAmtNum > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or rhc == '' or sc == '' or hh == '' or pop == '' or \
                          distMth == '' or distYr == '' or pregAttend == '' or pregTest == '' or pregPos == ''):
        checkStr = f"row - {row} | Incomplete data"
        ancCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(ANC) sheet', checkStr])
      
      if llinAmtNum <= 0 and (rhc != '' or sc != '' or vname != '' or hh != '' or pop != '' or \
                          distMth != '' or distYr != '' or pregAttend != '' or pregTest != '' or pregPos != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        ancCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(ANC) sheet', checkStr])
      
      if llinAmtNum > 0 and llinAmtNum > pregAttendNum:
        checkStr = f"row - {row} | Distributed LLIN is more than pregnant women attended"
        ancCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(ANC) sheet', checkStr])
        
      ancCheck = ancCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'LLIN dist(ANC) sheet', row, rpMth, rpYr)
      
    if len(ancCheck) == 0:
      ancCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(ANC) sheet', 'OK'])
  else:
    checkStr = "No data"
    ancCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(ANC) sheet', checkStr])
  return ancCheck

def checkLlinOther(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  llinOtherData = data["'LLIN dist(Other)'"]['data']
  # print(json.dumps(llinOtherData, indent=4))
  dataStartRow = int(data["'LLIN dist(Other)'"]['headerRow']) + 1
  llinOtherCheck = []
  if len(llinOtherData) > 0:
    for row, llinOther in enumerate(llinOtherData, start=dataStartRow):
      org = llinOther["Organization"]
      sr = llinOther["State/Region"]
      tsp = llinOther["Township"]
      rpMth = llinOther["Reporting month"]
      rpYr = llinOther["Reporting year"]
      desc = llinOther["Description"]
      date = llinOther["Date of distribution"]
      llinAmt = llinOther["# of LLIN distributed"]
      rmk = llinOther["Remark"]

      llinAmtNum = convert2float(llinOther["# of LLIN distributed"])

      if llinAmtNum > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or desc == '' or date == ''):
        checkStr = f"row - {row} | Incomplete data"
        llinOtherCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(Other) sheet', checkStr])

      if llinAmtNum <= 0 and (desc != '' or date != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        llinOtherCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(Other) sheet', checkStr])

      llinOtherCheck = llinOtherCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'LLIN dist(Other) sheet', row, rpMth, rpYr)
      
    if len(llinOtherCheck) == 0:
      llinOtherCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(Other) sheet', 'OK'])
  else:
    checkStr = "No data"
    llinOtherCheck.append([mainOrg, mainSr, mainTsp, 'LLIN dist(Other) sheet', checkStr])
  return llinOtherCheck

def checkRecruitment(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  recruitData = data["Recruitment"]['data']
  # print(json.dumps(recruitData, indent=4))
  dataStartRow = int(data['Recruitment']['headerRow']) + 1
  recruitCheck = []
  if len(recruitData) > 0:
    for row, recruit in enumerate(recruitData, start=dataStartRow):
      org = recruit["Organization"]
      sr = recruit["State/Region"]
      tsp = recruit["Township"]
      rpMth = recruit["Reporting month"]
      rpYr = recruit["Reporting year"]
      wpNum = recruit["Work plan activity number"]
      activityDesc = recruit["Activity description"]
      pos = recruit["Position"]
      providerCode = recruit["Code of provider (if available)"]
      providerName = recruit["Name of provider"]
      vc = recruit["Village code if available"]
      rhc = recruit["RHC"]
      sc = recruit["Subcenter"]
      vname = recruit["Village"]
      sDate = recruit["Start date"]
      eDate = recruit["End date"]
      status = recruit["Status"]

      if wpNum != '' or activityDesc != '' or pos != '' or providerCode != '' or providerName != '' or vc != '' or rhc != '' or sc != '' or vname != '' or sDate != '' or eDate != '' or status != '':
        if org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or wpNum == '' or activityDesc == '' or pos == '' or sDate == '' or status == '':
#           print(recruit)
          checkStr = f"row - {row} | Incomplete data"
          recruitCheck.append([mainOrg, mainSr, mainTsp, 'Recruitment sheet', checkStr])

      if status == 'Removed' and eDate == '':
#         print("checking removed status")
#         print(recruit)
        checkStr = f"row - {row} | Incomplete data"
        recruitCheck.append([mainOrg, mainSr, mainTsp, 'Recruitment sheet', checkStr])

      recruitCheck = recruitCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'Recruitment sheet', row, rpMth, rpYr)
      if vc!= '':
        recruitCheck = recruitCheck + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, 'Recruitment sheet', row, vc, rhc,sc,vname)
      
    if len(recruitCheck) == 0:
      recruitCheck.append([mainOrg, mainSr, mainTsp, 'Recruitment sheet', 'OK'])
  else:
    checkStr = "No data"
    recruitCheck.append([mainOrg, mainSr, mainTsp, 'Recruitment sheet', checkStr])
  return recruitCheck

def checkC19(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  c19Data = data["'C19 material distribution'"]['data']
  # print(json.dumps(c19Data, indent=4))
  dataStartRow = int(data["'C19 material distribution'"]['headerRow']) + 1
  c19Check = []
  if len(c19Data) > 0:
    for row, c19 in enumerate(c19Data, start=dataStartRow):
      org = c19["Organization"]
      sr = c19["State/Region"]
      tsp = c19["Township"]
      rpMth = c19["Reporting month"]
      rpYr = c19["Reporting year"]
      wpNum = c19["Work plan activity number"]
      activity = c19["Activity title/topic"]
      item = c19["Included items"]
      icmv = c19["# of ICMVs receiving C19 prevention materials"]
      gp = c19["# of GPs receiving C19 prevention materials"]
      mobile = c19["# of mobile teams receiving C19 prevention materials"]

      icmv = convert2float(c19["# of ICMVs receiving C19 prevention materials"])
      gp = convert2float(c19["# of GPs receiving C19 prevention materials"])
      mobile = convert2float(c19["# of mobile teams receiving C19 prevention materials"])

      if (icmv > 0 or gp > 0 or mobile > 0) and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or wpNum == '' or activity == '' or item == '' or icmv == '' or gp == '' or mobile == ''):
        checkStr = f"row - {row} | Incomplete data"
        c19Check.append([mainOrg, mainSr, mainTsp, 'C19 material distribution sheet', checkStr])

      if icmv <= 0 and gp <= 0 and mobile <= 0 and (wpNum != '' or activity != '' or item != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        c19Check.append([mainOrg, mainSr, mainTsp, 'C19 material distribution sheet', checkStr])

      c19Check = c19Check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'C19 material distribution sheet', row, rpMth, rpYr)
      
    if len(c19Check) == 0:
      c19Check.append([mainOrg, mainSr, mainTsp, 'C19 material distribution sheet', 'OK'])
  else:
    checkStr = "No data"
    c19Check.append([mainOrg, mainSr, mainTsp, 'C19 material distribution sheet', checkStr])
  return c19Check

def checkIecDist(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  iecData = data["'IEC,material distribution'"]['data']
  # print(json.dumps(iecData, indent=4))
  dataStartRow = int(data["'IEC,material distribution'"]['headerRow']) + 1
  iecCheck = []
  if len(iecData) > 0:
    for row, iec in enumerate(iecData, start=dataStartRow):
      org = iec["Organization"]
      sr = iec["State/Region"]
      tsp = iec["Township"]
      rpMth = iec["Reporting month"]
      rpYr = iec["Reporting year"]
      wpNum = iec["Work plan activity number"]
      activity = iec["Activity title/topic"]
      item = iec["Item description"]
      activityDesc = iec["Activity description"]
      amt = iec["Number of items distributed"]
      unit = iec["unit"]
      rmk = iec["Remark"]
      
      amtNum = convert2float(iec["Number of items distributed"])

      if amtNum > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or wpNum == '' or activity == '' or item == '' or amt == '' or unit == ''):
        checkStr = f"row - {row} | Incomplete data"
        iecCheck.append([mainOrg, mainSr, mainTsp, 'IEC,material distribution sheet', checkStr])
        
      if amtNum <= 0 and (wpNum != '' or activity != '' or item != '' or amt != '' or unit != ''):
        checkStr = f"row - {row} | 0 or blank activity data doesn't need to be mentioned in the report"
        iecCheck.append([mainOrg, mainSr, mainTsp, 'IEC,material distribution sheet', checkStr])

      iecCheck = iecCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'IEC,material distribution sheet', row, rpMth, rpYr)
      
    if len(iecCheck) == 0:
      iecCheck.append([mainOrg, mainSr, mainTsp, 'IEC,material distribution sheet', 'OK'])
  else:
    checkStr = "No data"
    iecCheck.append([mainOrg, mainSr, mainTsp, 'IEC,material distribution sheet', checkStr])
  return iecCheck

def checkCommodityDist(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  commoData = data["'RDT,ACT,CQ,PQ distribution'"]['data']
  # print(json.dumps(commoData, indent=4))
  dataStartRow = int(data["'RDT,ACT,CQ,PQ distribution'"]['headerRow']) + 1
  commoCheck = []
  if len(commoData) > 0:
    for row, commo in enumerate(commoData, start=dataStartRow):
      org = commo["Organization"]
      rpMth = commo["Reporting month"]
      rpYr = commo["Reporting year"]
      dest = commo["Destination"]
      wpNum = commo["Work plan activity number"]
      activity = commo["Activity title/topic"]
      amt = commo["Number of items distributed"]
      unit = commo["unit"]
      rmk = commo["Remark"]

      amtNum = convert2float(commo["Number of items distributed"])

      if amtNum > 0 or (amtNum <=0 and (dest != '' or wpNum != '' or activity != '' or unit != '' or rmk != '')):
        checkStr = f"row - {row} | Do not need to report this activity by State/Region or Township team. This is URC central logistic team's activity"
        commoCheck.append([mainOrg, mainSr, mainTsp, 'RDT,ACT,CQ,PQ distribution sheet', checkStr])

      commoCheck = commoCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'RDT,ACT,CQ,PQ distribution sheet', row, rpMth, rpYr)
      
    if len(commoCheck) == 0:
      commoCheck.append([mainOrg, mainSr, mainTsp, 'RDT,ACT,CQ,PQ distribution sheet', 'OK'])
  else:
    checkStr = "No data"
    commoCheck.append([mainOrg, mainSr, mainTsp, 'RDT,ACT,CQ,PQ distribution sheet', checkStr])
  return commoCheck

def checkProcurement(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  procureData = data["procurement"]['data']
  # print(json.dumps(procureData, indent=4))
  dataStartRow = int(data['procurement']['headerRow']) + 1
  procureCheck = []
  if len(procureData) > 0:
    for row, procure in enumerate(procureData, start=dataStartRow):
      org = procure["Organization"]
      rpMth = procure["Reporting month"]
      rpYr = procure["Reporting year"]
      wpNum = procure["Work plan activity number"]
      activity = procure["Activity title/topic"]
      item = procure["Item"]
      amt = procure["Number of items procured/purchased"]
      rmk = procure["Remark"]

      amtNum = convert2float(procure["Number of items procured/purchased"])

      if amtNum > 0 or (amtNum <=0 and (wpNum != '' or activity != '' or item != '')):
        checkStr = f"row - {row} | Do not need to report this activity by State/Region or Township team. This is URC and Partner central teams' activity"
        procureCheck.append([mainOrg, mainSr, mainTsp, 'procurement sheet', checkStr])
      procureCheck = procureCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'procurement sheet', row, rpMth, rpYr)
      
    if len(procureCheck) == 0:
      procureCheck.append([mainOrg, mainSr, mainTsp, 'procurement sheet', 'OK'])
  else:
    checkStr = "No data"
    procureCheck.append([mainOrg, mainSr, mainTsp, 'procurement sheet', checkStr])
  return procureCheck

def checkCboSupport(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  cboData = data["'CBO,CSG,EHO support'"]['data']
  # print(json.dumps(cboData, indent=4))
  dataStartRow = int(data["'CBO,CSG,EHO support'"]['headerRow']) + 1
  cboCheck = []
  if len(cboData) > 0:
    for row, cbo in enumerate(cboData, start=dataStartRow):
      org = cbo["Organization"]
      sr = cbo["State/Region"]
      tsp = cbo["Township"]
      rpMth = cbo["Reporting month"]
      rpYr = cbo["Reporting year"]
      wpNum = cbo["Work plan activity number"]
      activity = cbo["Activity title/topic"]
      orgSupport = cbo["Name of organization supported"]
      category = cbo["Category of organization (CBO,CSG,EHO)"]
      detail = cbo["Mention detail support"]

      if (wpNum != '' or activity != '' or orgSupport != '' or category != '' or detail != ''):
        if org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or wpNum == '' or activity == '' or orgSupport == '' or category == '' or detail == '':
          checkStr = f"row - {row} | Incomplete data"
          cboCheck.append([mainOrg, mainSr, mainTsp, 'CBO,CSG,EHO support sheet', checkStr])

      cboCheck = cboCheck + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, 'CBO,CSG,EHO support sheet', row, rpMth, rpYr)
      
    if len(cboCheck) == 0:
      cboCheck.append([mainOrg, mainSr, mainTsp, 'CBO,CSG,EHO support sheet', 'OK'])
  else:
    checkStr = "No data"
    cboCheck.append([mainOrg, mainSr, mainTsp, 'CBO,CSG,EHO support sheet', checkStr])
  return cboCheck

def checkDesignDevelop(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  nameOfSheet = "'Design,develop'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      rpMth = sData["Reporting month"]
      rpYr = sData["Reporting year"]
      wpNum = sData["Work plan activity number"]
      activity = sData["Activity title/topic"]
      sDate = sData["Activity start date"]
      eDate = sData["Activity completion date"]
      status = sData["Activity status"]
      rmk = sData["Remark"]

      if wpNum != '' or activity != '' or sDate != '' or status != '':
        if org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or wpNum == '' or activity == '' or sDate == '' or status == '':
          checkStr = f"row - {row} | Incomplete data"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      check = check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkStudyAssessmentSurvey(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  nameOfSheet = "'Study,assessment,survey'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      wpNum = sData["Work plan activity number"]
      activity = sData["Activity title/topic"]
      sDate = sData["Activity start date"]
      eDate = sData["Activity end date"]
      status = sData["Activity status"]
      fDate = sData["Report finalized/disseminated date"]
      rmk = sData["Remark"]

      if wpNum != '' or activity != '' or sDate != '' or eDate != '' or status != '' or fDate != '':
        if org == '' or sr == '' or tsp == '' or wpNum == '' or activity == '' or sDate == '' or status == '':
          checkStr = f"row - {row} | Incomplete data"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkVisit(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  nameOfSheet = "visits"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      wpNum = sData["Work plan activity number"]
      activity = sData["Activity title/topic"]
      vBy = sData["Visit by"]
      vTo = sData["Visit to"]
      date = sData["Date of activity"]
      duration = sData["Duration (in days) of visit"]
      cwith = sData["In collaboration with"]
      detail = sData["Visit detail information (including data)"]
      
      if (vBy != '' or vTo != '') and (org == '' or sr == '' or tsp == '' or wpNum == '' or activity == '' or vBy == '' or vTo == '' or date == '' or duration == '' or detail == ''):
        checkStr = f"row - {row} | Incomplete data"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkTMW(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  nameOfSheet = "'Training,Meeting,Workshop'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      rpMth = sData["Reporting month"]
      rpYr = sData["Reporting year"]
      wpNum = sData["Work plan activity number"]
      rpLvl = sData["Report level"]
      location = sData["Location of activity"]
      activity = sData["Activity title/topic"]
      date = sData["Date of activity"]
      duration = sData["Duration (in days) of activity"]
      male = sData["Number of participants (Male)"]
      female = sData["Number of participants (Female)"]
      total = sData["Number of participants (Total)"]
      rpDate = sData["Date of activity report submission"]
      note = sData["Meeting note"]
      rmk = sData["Remark"]
      
      duration = convert2float(sData["Duration (in days) of activity"])
      male = convert2float(sData["Number of participants (Male)"])
      female = convert2float(sData["Number of participants (Female)"])
      total = convert2float(sData["Number of participants (Total)"])
      
      if male + female != total:
        checkStr = f"row - {row} | Total participant calculation error"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      if total > 0 and duration <= 0:
        checkStr = f"row - {row} | Duration (in days) must be number and at least 1"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      if total > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or wpNum == '' or rpLvl == '' or location == '' or \
                        activity == '' or date == '' or duration == '' or male == '' or female == '' or total == ''):
        checkStr = f"row - {row} | Incomplete data"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      check = check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkMSS(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  providerData = data["All_provider"]['data']
  nameOfSheet = "'Meeting,supervision,stockout'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      rpMth = sData["Reporting month"]
      rpYr = sData["Reporting year"]
      personCode = sData["Person code"]
      providerType = sData["Type of provider"]
      providerName = sData["Name of provider"]
      sex = sData["Sex"]
      vc = sData["Assigned village code"]
      vname = sData["Assigned village name"]
      mv = sData["Monitoring visit"]
      mvDate = sData["Date of monitoring visit"]
      mvRDTso = sData["MV_RDT stock out"]
      mvACTso = sData["MV_ACT stock out"]
      mvCQso = sData["MV_CQ stock out"]
      mvPQso = sData["MV_PQ stock out"]
      osdc = sData["On-site data collection"]
      osdcRDTso = sData["OSDC_RDT stock out"]
      osdcACTso = sData["OSDC_ACT stock out"]
      osdcCQso = sData["OSDC_CQ stock out"]
      osdcPQso = sData["OSDC_PQ stock out"]
      meet = sData["Meeting_status"]
      meetRDTso = sData["Meeting_RDT stock out"]
      meetACTso = sData["Meeting_ACT stock out"]
      meetCQso = sData["Meeting_CQ stock out"]
      meetPQso = sData["Meeting_PQ stock out"]
      ov = sData["Other visit_specify visit"]
      ovRDTso = sData["OV_RDT stock out"]
      ovACTso = sData["OV_ACT stock out"]
      ovCQso = sData["OV_CQ stock out"]
      ovPQso = sData["OV_PQ stock out"]
      rmk = sData["Remark"]
      
      if personCode != '':
        try:
          apProviderType = providerData[personCode[:-2]][personCode]['Type_of_provider']
          apProviderName = providerData[personCode[:-2]][personCode]['Name_Of_Provider']
          apProviderSex = providerData[personCode[:-2]][personCode]['Sex']
          apProviderVc = providerData[personCode[:-2]][personCode]['Assigned_village_code']
          apProviderVname = providerData[personCode[:-2]][personCode]['Assigned_village_name']

          if personCode != '' and len(personCode) != 9:
            checkStr = f"row - {row} | Person code format error"
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])      
          if personCode != '' and len(personCode) == 9 and (providerType != apProviderType or providerName != apProviderName or sex != apProviderSex or vc != apProviderVc or vname != apProviderVname):
            checkStr = f"row - {row} | Person code, name, sex, assigned village code or assigned village name are not the same as those mentioned in All_provider sheet. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])      
          if personCode!= '' and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == ''):
            checkStr = f"row - {row} | Incomplete data." 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and mv != '' and (mvDate == '' or mvRDTso == '' or mvACTso == '' or mvCQso == '' or mvPQso == ''):
            checkStr = f"row - {row} | Monitoring visit - Incomplete data. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])      
          if personCode != '' and mv == '' and (mvDate != '' or mvRDTso != '' or mvACTso != '' or mvCQso != '' or mvPQso != ''):
            checkStr = f"row - {row} | Monitoring visit - Stockout data reported but monitoring visit not mentioned. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and osdc == "Y" and (osdcRDTso == '' or osdcACTso == '' or osdcCQso == '' or osdcPQso == ''):
            checkStr = f"row - {row} | Onsite data collection - Incomplete data. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])      
          if personCode != '' and osdc == "" and (osdcRDTso != '' or osdcACTso != '' or osdcCQso != '' or osdcPQso != ''):
            checkStr = f"row - {row} | Onsite data collection - Stockout data reported without OSDC. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and meet == "Y" and (meetRDTso == '' or meetACTso == '' or meetCQso == '' or meetPQso == ''):
            checkStr = f"row - {row} | Monthly meeting - Incomplete data. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
          if personCode != '' and meet != "Y" and (meetRDTso != '' or meetACTso != '' or meetCQso != '' or meetPQso != ''):
            checkStr = f"row - {row} | Monthly meeting - Stockout data reported without attending monthly meeting. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and ov != '' and (ovRDTso == '' or ovACTso == '' or ovCQso == '' or ovPQso == ''):
            checkStr = f"row - {row} | Other visit - Incomplete data. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and ov == '' and (ovRDTso != '' or ovACTso != '' or ovCQso != '' or ovPQso != ''):
            checkStr = f"row - {row} | Other visit - Stockout data reported without mentioning the visit. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          check = check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)

        except:
          checkStr = "row - " + str(row) + " | Person code not found in All provider sheet"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkTrainingProvider(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  # print(data.keys())
  providerData = data["All_provider"]['data']
  nameOfSheet = "'Training attendance (Provider)'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    pmiIndicator = []
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      rpMth = sData["Reporting month"]
      rpYr = sData["Reporting year"]
      personCode = sData["Person code"]
      providerType = sData["Type of provider"]
      providerName = sData["Name of provider"]
      sex = sData["Sex"]
      nor = sData["New/Refresher"]
      replacement = sData["Is Replacement of provider"]
      onjob = sData["Is On-job training"]
      id = sData["Training id"]
      place = sData["Place of training"]
      sdate = sData["Training start date"]
      edate = sData["Training end date"]
      duration = sData["Training duration in days"]
      pmi = sData["Include in PMI required indicator"]
      dx = sData["Diagnosis"]
      cmx = sData["Case management"]
      od = sData["Other disease"]
      sbc = sData["SBC"]
      acd = sData["PACD/RACD"]
      ssm = sData["Severe symptom monitoring"]
      cifir = sData["CIFIR"]
      other = sData["Other (Specify topic)"]
      rmk = sData["Remark"]
      if personCode != '':
        try:
          apProviderType = providerData[personCode[:-2]][personCode]['Type_of_provider']
          apProviderName = providerData[personCode[:-2]][personCode]['Name_Of_Provider']
          apProviderSex = providerData[personCode[:-2]][personCode]['Sex']
          apProviderVc = providerData[personCode[:-2]][personCode]['Assigned_village_code']
          apProviderVname = providerData[personCode[:-2]][personCode]['Assigned_village_name']

          if personCode != '' and len(personCode) != 9:
            checkStr = f"row - {row} | Person code format error"
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
          if personCode != '' and len(personCode) == 9 and (providerType != apProviderType or providerName != apProviderName or sex != apProviderSex):
            checkStr = f"row - {row} | Person code, name, sex, assigned village code or assigned village name are not the same as those mentioned in All_provider sheet. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
          if personCode != '' and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or personCode == '' or providerType == '' or providerName == '' or sex == '' or nor == '' or \
                                   replacement == '' or onjob == '' or id == '' or place == '' or sdate == '' or edate == '' or duration == ''):
            checkStr = f"row - {row} | Incomplete data. ({personCode})" 
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and dx == 'full' and cmx == 'full' and pmi == 'Y':
            if not personCode in pmiIndicator:
              pmiIndicator.append(personCode)
            else:
              checkStr = f"row - {row} | {personCode} is already reported as PMI indicator before" 
              check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          if personCode != '' and dx == 'full' and cmx == 'full' and pmi != 'Y':
            if not personCode in pmiIndicator:
              checkStr = f"row - {row} | {personCode} received full package of Diagnosis and case management and has not been reported as PMI indicator. Suggested to report as PMI indicator or change 'full' to 'partial'" 
              check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

          check = check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)
        except:
          checkStr = "row - " + str(row) + " | Person code not found in All provider sheet. (" + personCode + ")" 
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  check.append([mainOrg, mainSr, mainTsp, 'Training PMI indicator summary sheet', 'Please check it manually'])
  return check

def checkCsg(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  nameOfSheet = "CSG"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    pmiIndicator = []
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      rpMth = sData["Reporting month"]
      rpYr = sData["Reporting year"]
      vc = sData["Location code"]
      rhc = sData["RHC"]
      sc = sData["Subcenter"]
      vname = sData["Village name"]
      date = sData["Date"]
      csgName = sData["Name of CSG"]
      csgMem = sData["No. CSG members"]
      csgAttend = sData["No. Participants attended"]
      mcm = sData["Monthly CSG committee meeting"]
      ics = sData["ICMV & community support activities"]
      ela = sData["Engagement with local authority for CSG activities"]
      cbhs = sData["Collaborating MW/ BHS in PHC activities"]
      ethd = sData["Engagement with Township Health Department/ VBDC"]
      er = sData["Emergency referral"]
      rmk = sData["Remark"]

      csgMemNum = convert2float(sData["No. CSG members"])
      csgAttendNum = convert2float(sData["No. Participants attended"])
      
      if csgAttendNum > 0 and csgMemNum < csgAttendNum:
        checkStr = f"row - {row} | Number of attended members are greater than number of CSG members"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      
      if csgAttendNum > 0 and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or vc == '' or rhc == '' or sc == '' or vname == '' or date == '' or csgMem == '' or \
                               mcm == '' or ics == '' or ela == '' or cbhs == '' or ethd == '' or er == ''):
        checkStr = f"row - {row} | Incomplete data"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      check = check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)
      check = check + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, nameOfSheet + " sheet", row, vc, rhc, sc,vname)
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([[mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr]])
  return check

def checkCsgSmallGrant(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  nameOfSheet = "'CSG (small grant)'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    pmiIndicator = []
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      rpMth = sData["Reporting month"]
      rpYr = sData["Reporting year"]
      vc = sData["Location code"]
      rhc = sData["RHC"]
      sc = sData["Subcenter"]
      vname = sData["Village name"]
      csgName = sData["Name of CSG"]
      smallGrant = sData["Small grant support (Y/N)"]
      date = sData["Date of support"]

      if smallGrant == "Y" and (org == '' or sr == '' or tsp == '' or rpMth == '' or rpYr == '' or vc == '' or rhc == '' or sc == '' or vname == '' or date == ''):
        checkStr = f"row - {row} | Incomplete data"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      check = check + checkRpPeriod (verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)
      check = check + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, nameOfSheet + " sheet", row, vc, rhc, sc,vname)
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkIcmvOtherDisease(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  nameOfSheet = "'ICMV other disease'"
  resData = data[nameOfSheet]['data']
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State/Region"]
      tsp = sData["Township"]
      personCode = sData["Person code"]
      providerType = sData["Type of provider"]
      quarter = sData["Quarter of Fiscal Year"]
      fy = sData["Fiscal Year (Enter only the year number)"]
      mal1 = sData["(MAL)_Number of RDTs taken and read (Do not count invalid RDTs.)"]
      mal2 = sData["(MAL)_Number of confirmed malaria cases "]
      mal3 = sData["(MAL)_Number of confirmed malaria cases reported within 24 hours of onset of fever   "]
      mal4 = sData["(MAL)_Number of confirmed falciparum malaria cases receiving DOT "]
      mal5 = sData["(MAL)_Number of severe malaria cases referred"]
      mal6 = sData["(MAL)_Number of confirmed malaria cases underwent the blood recheck after 28 days"]
      mal7 = sData["(MAL)_Number of people received health education (Malaria)"]
      dhf1 = sData["(DHF)_Number of suspected cases referred for dengue "]
      dhf2 = sData["(DHF)_Number of people received health education (Dengue)"]
      fal1 = sData["(Filariasis)_Number of suspected cases referred for Lymphatic Filariasis "]
      fal2 = sData["(Filariasis)_Number of people with Filariasis educated for prevention of complication"]
      fal3 = sData["(Filariasis)_Number of people received health education (Filariasis)"]
      tb1 = sData["(TB)_Number  of referral cases for TB screening \n"]
      tb2 = sData["(TB)_Number of notified TB cases (all forms)  \n"]
      tb3 = sData["(TB)_Number of TB cases receiving DOT"]
      tb4 = sData["(TB)_Number of people received health education (TB)"]
      hiv1 = sData["(STI_HIV)_Number of condom distributed for HIV prevention"]
      hiv2 = sData["(STI_HIV)_Number of people referred for HIV testing "]
      hiv3 = sData["(STI_HIV)_Number of people referred for STI treatment"]
      hiv4 = sData["(STI_HIV)_Number of people received health education (STI/HIV)"]
      lep1 = sData["(Leprosy)_Number of suspected cases referred for leprosy"]
      lep2 = sData["(Leprosy)_Number of leprosy cases referred for drug reaction & complication "]
      lep3 = sData["(Leprosy)_Number of people received health education (Leprosy)"]

      if personCode != '' and (org == '' or sr == '' or tsp == '' or providerType == '' or quarter == '' or fy == '' or mal1 == '' or mal2 == '' or mal3 == '' or mal4 == '' or mal5 == '' or mal6 == '' or mal7 == '' or \
                               dhf1 == '' or dhf2 == '' or fal1 == '' or fal2 == '' or fal3 == '' or tb1 == '' or tb2 == '' or tb3 == '' or tb4 == '' or hiv1 == '' or hiv2 == '' or hiv3 == '' or hiv4 == '' or \
                               lep1 == '' or lep2 == '' or lep3 == ''):
        checkStr = f"row - {row} | Incomplete data"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      check = check + checkProviderType(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, nameOfSheet + ' sheet', row, personCode, providerType)
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  check.append([mainOrg, mainSr, mainTsp, 'Waste disposal sheet', 'Please check manually'])
  check.append([mainOrg, mainSr, mainTsp, 'Expired drug sheet', 'Please check manually'])
  return check

def checkPatientRecord(verifyFindingSheet, mainOrg, mainSr, mainTsp, data):
  vilData = data['All_villages']['data']
  providerData = data['All_provider']['data']
  nameOfSheet = "'Patient record'"
  resData = data[nameOfSheet]['data']
  
  month_mapping = {
        'January': 1,
        'February': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'August': 8,
        'September': 9,
        'October': 10,
        'November': 11,
        'December': 12
    }
  # print(json.dumps(resData, indent=4))
  dataStartRow = int(data[nameOfSheet]['headerRow']) + 1
  check = []
  if len(resData) > 0:
    for row, sData in enumerate(resData, start=dataStartRow):
      org = sData["Organization"]
      sr = sData["State Region"]
      tsp = sData["Township OD"]
      providerType = sData["Type of Provider"]
      rpBy = sData["Reported By"]
      activity = sData["Activity"]
      rCaseId = sData["Response to Case ID"]
      cblRhc = sData["RHC in carbonless heading"]
      cblSc = sData["Subcenter in carbonless heading"]
      cblAddr = sData["Address in carbonless heading"]
      cblMth = sData["Month in Carbonless"]
      cblYr = sData["Year in Carbonless"]
      cblYr = str(cblYr)
      cblYr = cblYr.replace(",","")
#       cblYr = int(cblYr)
      cblPg = sData["Carbonless Page No."]
      cblRow = sData["Carbonless Row No."]
      vc = sData["Village or Location Code of patient address if available"]
      rhc = sData["RHC"]
      sc = sData["Sub-center"]
      rpMth = sData["Reporting Month"]
      rpYr = sData["Reporting Year"]
      date = sData["Tested Date"]
      name = sData["Name"]
      age = sData["Age Year"]
      vname = sData["Address"]
      popType = sData["Population Type"]
      sex = sData["Sex"]
      preg = sData["Pregnancy Month (Lactating mother - (-1))"]
      testType = sData["Test Type"]
      numVisit = sData["Number of Visit"]
      testResult = sData["Test Result"]
      dx = sData["Diagnosis"]
      act = sData["Number of ACT tab treated (not indicated = 77)"]
      cq = sData["Number of CQ tab treated (not indicated = 77)"]
      pq75 = sData["Number of PQ7.5mg tab treated (not indicated = 77) (Patient is treated with PQ15mg = 99)"]
      pq15 = sData["Number of PQ15mg tab treated (not indicated = 77) (Patient is treated with PQ7.5mg = 99)"]
      lt24 = sData["Less Than 24hrs"]
      refer = sData["Referred"]
      death = sData["Death"]
      travel = sData["Travel history"]
      occu = sData["Occupation"]
      he = sData["Health Education"]
      heBy = sData["HE By"]
      pvNpv = sData["Village Categorization"]
      temp = sData["Temperature (\u02daF) e.g. 98.6, 100"]
      deBy = sData["Data Entry By"]
      dotStatus = sData["DOT status"]
      dotCat = sData["DOT category"]
      dotRpMth = sData["Reporting month of DOT form submission"]
      dotProvider = sData["Code of DOT provider"]
      dotReason = sData["Reason for DOT Incomplete or not enrolled"]
      rmk = sData["Remark"]
      caseId = sData["Malaria case ID (For elimination townships)"]

      ageNum = convert2float(sData["Age Year"])

      if tsp != '' or testResult != '':
        if cblYr != '' and cblMth != '' and date != '':
          rpPeriodRpYr = rpYr
          rpPeriodRpYr = str(rpPeriodRpYr).replace(",","")
          rpPeriodRpYr = int(rpPeriodRpYr)
          rpPeriodRpMth = rpMth
          rpPeriodRpMthNum = month_mapping[rpPeriodRpMth] + 1
          if rpPeriodRpMthNum == 13:
            rpPeriodRpMthNum = 1
            rpPeriodRpYr = rpPeriodRpYr + 1          
          rpPeriodEnd = datetime(rpPeriodRpYr, rpPeriodRpMthNum, 1) - timedelta(days=1)
          
          cblYrCalc = int(cblYr)
          cblMthNum = month_mapping[cblMth] + 1
          if cblMthNum == 13:
            cblMthNum = 1
            cblYrCalc = cblYrCalc + 1
          cblPeriodEnd = datetime(cblYrCalc, cblMthNum, 1) - timedelta(days=1)
          testTimeStamp = datetime.strptime(date, '%d-%b-%Y')  
          if testTimeStamp > cblPeriodEnd:        
            checkStr = f"row - {row} | Blood testing date and month in carbonless is not consistent. Month in carbonless - {cblMth} | Year in carbonless - {cblYr} | Test date - {date}"
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
          if cblPeriodEnd > rpPeriodEnd:
            checkStr = f"row - {row} | Carbonless month and Year must not be later than reporting month and Year. Carbonless month-year - {cblMth}-{cblYr} | Reporting month-year - {rpMth}-{rpYr}"
            check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        else:
          checkStr = f"row - {row} | One of month in carbonless, year in carbonless and/or test date is blank."
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if org == '' or sr == '' or tsp == '' or providerType == '' or rpBy == '' or activity == '' or cblRhc == '' or cblSc == '' or cblAddr == '' or cblMth == '' or cblYr == '' or cblPg == '' or cblRow == '' or \
            vc == '' or rhc == '' or sc == '' or rpMth == '' or rpYr == '' or date == '' or name == '' or age == '' or vname == '' or popType == '' or sex == '' or testType == '' or numVisit == '' or testResult == '' or \
            pvNpv == '' or deBy == '':
          checkStr = f"row - {row} | Incomplete data"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if preg != '' and sex != 'F':
          checkStr = f"row - {row} | Pregnant but not female"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if (activity == 'RACD-CIFIR' or activity == 'RACD' or activity == 'Followup response (within 3rd-5th week)' or activity == 'D28 followup response') and rCaseId == '':
          checkStr = f"row - {row} | Response to case ID data required for RACD, RACD-CIFIR, Followup response (within 3rd-5th week) or D28 followup response"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if activity != 'RACD-CIFIR' and activity != 'RACD' and activity != 'Followup response (within 3rd-5th week)' and activity != 'D28 followup response' and rCaseId != '':
          checkStr = f"row - {row} | Response to case ID data mentioned for activity that is not one of RACD, RACD-CIFIR, Followup response (within 3rd-5th week) or D28 followup response"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if he == "":
          checkStr = f"row - {row} | Incomplete data in HE column."
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if he == 'Y' and heBy == '':
          checkStr = f"row - {row} | HE 'Y' but HE provider not mentioned"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if testResult != 'Negative' and refer != 'Y' and (dx == '' or act == '' or cq == '' or pq75 == '' or pq15 == '' or lt24 == '' or refer == '' or death == '' or travel == '' or occu == '' or dotStatus == '' or caseId == ''):
          checkStr = f"row - {row} | Incomplete data - Positive not referred patient"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if testResult != 'Negative' and refer == 'Y' and (dx == '' or lt24 == '' or refer == '' or death == '' or travel == '' or occu == '' or caseId == '' or dotStatus == ''):
          checkStr = f"row - {row} | Incomplete data - Positive referred patient"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if testResult != 'Negative' and dotStatus == 'Complete' and (dotCat == '' or dotRpMth == '' or dotProvider == ''): 
          checkStr = f"row - {row} | Incomplete data - DOT section"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
        if testResult != 'Negative' and (dotStatus == 'Incomplete' or dotStatus == 'Not enrolled') and dotReason == '': 
          checkStr = f"row - {row} | Incomplete data - DOT section"
          check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])

      check = check + checkRpPeriod(verifyFindingSheet, mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', row, rpMth, rpYr)
      check = check + checkVC(verifyFindingSheet, mainOrg, mainSr, mainTsp, data, nameOfSheet + ' sheet',row,vc,rhc,sc,vname)
      try:
        if (providerType == 'ICMV-V' or providerType == 'ICMV-W' or providerType == 'GP') and org != 'NMCP' and org != 'NMCP/URC' and org != 'URC/NMCP':
          providerPostCode = rpBy[:-2]
          providerVc = providerData[providerPostCode][rpBy]['Assigned_village_code']
          if providerVc != '':
            providerRhc = vilData[providerVc]['RHC_Name']
            providerSc = vilData[providerVc]['Sub-center_Name']
            providerVname = vilData[providerVc]['Name_of_Village']
            if cblRhc != providerRhc or cblSc != providerSc or cblAddr != providerVname:
              checkStr = f"row - {row} | Provider RHC, SC, Village name/Address is not the same as those mentioned in All villages and All provider sheet. {rpBy} | {providerVc}, RHC - '{cblRhc}'|'{providerRhc}', SC - '{cblSc}'|'{providerSc}', Addredd - '{cblAddr}'|'{providerVname}'"
              check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      except:
#         print(json.dumps(sData, indent=4))
        checkStr = f"row - {row} | Person code not found in All provider sheet ({rpBy})"
        check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
      
    if len(check) == 0:
      check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', 'OK'])
  else:
    checkStr = "No data"
    check.append([mainOrg, mainSr, mainTsp, nameOfSheet + ' sheet', checkStr])
  return check

def checkDataValidation(verifyFindingSheet, mainOrg, mainSr, mainTsp, allData, ddRule, nRule, dRule):
  dvCheck = []
  for sheetName in allData:
    if sheetName != 'All_villages' and sheetName != 'All_provider':
      data = allData[sheetName]['data']
      dataStartRow = int(allData[sheetName]['headerRow']) + 1
    elif sheetName == 'All_villages':
      # print(json.dumps(allData[sheetName],indent=4))
      data = []
      for vilData in allData[sheetName]['data']:
        vD = allData[sheetName]['data'][vilData]
        vD["Village_code"] = vilData
        data.append(vD)
      dataStartRow = 2
    elif sheetName == 'All_provider':
      data = []
      for postCode in allData[sheetName]['data']:
        for personCode in allData[sheetName]['data'][postCode]:
          aP = allData[sheetName]['data'][postCode][personCode]
          aP["Person_code"] = personCode
          aP["Post_code"] = postCode
          data.append(aP)
      dataStartRow = 3
    check = []
    for row, rowData in enumerate(data, start = dataStartRow):
      if (sheetName == 'All_villages' or sheetName == 'All_provider'):
        if rowData['Removed_date'] != '':
          continue
      # print(data)
      # print(rowData)
      for headText in rowData:
        avVc = ''
        apPerson = ''
        apPost = ''
        if sheetName == 'All_provider':
          apPerson = rowData['Person_code']
          apPost = rowData['Post_code']
        if sheetName == 'All_villages':
          avVc = rowData['Village_code']
        if rowData[headText] != '':
          # Checking dropdown validation
          if sheetName in ddRule:
            if headText in ddRule[sheetName]:
              ddList = ddRule[sheetName][headText]['list']
              # print(sheetName + " | " + headText + " | " + str(rowData[headText]))
              value2check = str(rowData[headText]).replace(",","")
              if not value2check in ddList:
#                 print(rowData)
                checkStr = "row - {rowNum} | Dropdown data validation check - invalid data in {headText1} column. Current value - {value}".format(rowNum=row,headText1=headText,value=rowData[headText])
                if sheetName == 'All_provider':
                  checkStr = avVc + " | " + checkStr
                if sheetName == 'All_provider':
                  checkStr = apPerson + " | " + apPost + " | " + checkStr
#                 checkStr = "row - " + str(row) + " | Dropdown data validation check - invalid data in " + headText + " column. Current value - " + rowData[headText]
                check.append([mainOrg, mainSr, mainTsp, sheetName + " Sheet", checkStr])
          
          # Checking number validation
          if sheetName in nRule:
            if headText in nRule[sheetName]:
              nValue = float(nRule[sheetName][headText]['list'][0])
              nvalue2check = str(rowData[headText]).replace(",",'')
              try:
                numV2C = float(nvalue2check)
              except:
                numV2C = -10.0
              
              if numV2C < nValue:
#                 print(rowData)
                checkStr = "row - " + str(row) + " | Number validation check - invalid data in " + headText + " column. Current value - " + str(nvalue2check) + ". Value must be >= " + str(nValue)
                if sheetName == 'All_provider':
                  checkStr = avVc + " | " + checkStr
                if sheetName == 'All_provider':
                  checkStr = apPerson + " | " + apPost + " | " + checkStr
                check.append([mainOrg, mainSr, mainTsp, sheetName + " Sheet", checkStr])

          # Check date validation
          if sheetName in dRule:
            if headText in dRule[sheetName]:
              dValue = dRule[sheetName][headText]['list'][0]
              dvalue2check = rowData[headText]
              try:
                dV2C = datetime.strptime(dvalue2check, '%m/%d/%Y')
              except:
                try:
                  dV2C = datetime.strptime(dvalue2check, '%d-%b-%Y')
                except:
                  dV2C = datetime.strptime('1/1/1900', '%m/%d/%Y')
              
              if dV2C < dValue:
#                 print(rowData)
                checkStr = "row - " + str(row) + " | Date validation check - invalid data in " + headText + " column. Current value - " + str(dvalue2check) + ". Value must be on or after " + dValue.strftime("%d-%b-%Y")
                if sheetName == 'All_provider':
                  checkStr = avVc + " | " + checkStr
                if sheetName == 'All_provider':
                  checkStr = apPerson + " | " + apPost + " | " + checkStr
                check.append([mainOrg, mainSr, mainTsp, sheetName + " Sheet", checkStr])
    if len(check) == 0:
        check.append([mainOrg, mainSr, mainTsp, sheetName + ' Sheet', 'Dropdown, number and date validation check - OK'])
    dvCheck = dvCheck + check
  return dvCheck

def list_of_lists_to_list_of_dicts(list_of_lists):
    keys = list_of_lists[0]
    list_of_dicts = []
    for values in list_of_lists[1:]:
        if len(values) < len(keys):
            values += [''] * (len(keys) - len(values))
        dict_from_list = dict(zip(keys, values))
        list_of_dicts.append(dict_from_list)
    return list_of_dicts

def getSheetData(service, url):
  spreadsheet = service.open_by_url(url)
  worksheets = spreadsheet.worksheets()
  sheetTitles = [sheet.title for sheet in worksheets]
  ranges = spreadsheet.values_batch_get(sheetTitles)
  ranges = ranges['valueRanges']
  data = {}
  for range in ranges:
    title = range['range']
    title = title.split("!")[0]
    # Convert numeric values to integers or floats based on their type
    for row in range['values']:
        for i, cell_value in enumerate(row):
            try:
                float_value = float(cell_value)
                if float_value.is_integer():
                    row[i] = int(float_value)  # Convert to integer if it's an integer
                else:
                    row[i] = float_value  # Otherwise, keep it as a float
            except (ValueError, TypeError):
                pass  # If it's not a numeric value, leave it as is
    lod = list_of_lists_to_list_of_dicts(range['values'])
    data[title] = {}
    data[title]['lod'] = lod
    data[title]['lol'] = range['values']
  return data
    
def validata_or_verify_report(service_account_info, url_of_report_file, url_of_verification_file, sh_name):
  # keepVar = ["service_account_info", "url_of_report_file", "url_of_verification_file", "sh_name"]
  # for var_name in list(globals().keys()):
  #   if var_name not in keepVar:
  #     del globals()[var_name]
  # -----------------------------------------------------------------------------
  # function call
  # fixTool
  print(f"report file link: {url_of_report_file}")
  print(f"findings file link: {url_of_verification_file}")
  print(f"sheet name: {sh_name}")
  gc = gspread.service_account_from_dict(service_account_info)
  
  url_of_fix_tool = 'https://docs.google.com/spreadsheets/d/1ZfJFnP6GZSwwpXGeIv8r8B3GO_yfHHPWWi1jJp7tfhg/edit#gid=39027120'
  fixTool = getSheetData(gc, url_of_fix_tool)
  headers = fixTool['tbl_header_1']['lod']
  dvRules = fixTool['dv_dropdown_1']['lod']
  numRules = fixTool['dv_number_1']['lod']
  dateRules = fixTool['dv_date_1']['lod']
  dvRulesLol = fixTool['dv_dropdown_1']['lol']
  
  sheetListTmp = headers
  sheetListTmp
  sheetList = {}
  for sheetData in sheetListTmp:
    if sheetData['Target sheet'] != 'End':
      sheetList[sheetData['Target sheet']] = {}
      sheetList[sheetData['Target sheet']]['headerRow'] = sheetData['Target row']
  sheetList['Recruitment'] = {}
  sheetList['Recruitment']['headerRow'] = 1
  
  verificationFile = gc.open_by_url(url_of_verification_file)
  verifyFindingSheet = verificationFile.worksheet(sh_name)
  verifyFindingSheet.clear()
  verifyFindingSheet.append_rows([["Last verify at", "", "Last successful verify at", "", "Last verify status:"],["Organization","State/Region", "Township", "Sheet name", "Findings/Remark"]])
  # verifyFindingSheet.update(values = [["Organization","State/Region", "Township", "Sheet name", "Findings/Remark"]], range_name = "A1:E1")

  reportFile = gc.open_by_url(url_of_report_file)
  reportSheetList = reportFile.worksheets()
  reportSheetTitles = [rpsheet.title for rpsheet in reportSheetList]
  reportData = reportFile.values_batch_get(reportSheetTitles)
  reportData = reportData['valueRanges']
  tmpSet = {}
  for tmpData in reportData:
    shName = tmpData['range'].split("!")[0]
    # print(shName)
    tmpSet[shName] = tmpData['values']
  
  # print(tmpSet.keys())
  for sheetName in sheetList:
    # print(f"Present in tmpSet - {sheetName}")
    header = sheetList[sheetName]['headerRow'] - 1
    if header > 0:
      #this code will run
      del tmpSet[sheetName][:header]
    # Convert numeric values to integers or floats based on their type
    for row in tmpSet[sheetName]:
        for i, cell_value in enumerate(row):
            originalVal = cell_value
            modVal = cell_value.replace(",","")
            try:
                float_value = float(modVal)
                if float_value.is_integer():
                    row[i] = int(float_value)  # Convert to integer if it's an integer
                else:
                    row[i] = float_value  # Otherwise, keep it as a float
            except (ValueError, TypeError):
                if cell_value != originalVal:
                    print(f"value changed: from {originalVal} to {cell_value}")
                pass  # If it's not a numeric value, leave it as is
    sheetList[sheetName]['data'] = list_of_lists_to_list_of_dicts(tmpSet[sheetName])

  mainOrg = ''
  mainSr = ''
  mainTsp = ''

  allVillagesData = sheetList['All_villages']['data']
  allVillagesDataTmp = {}
  for villageData in allVillagesData:
    village_code = villageData['Village_Code']
    del villageData['Village_Code']
    allVillagesDataTmp[village_code] = villageData
    if mainOrg == '':
      mainOrg = villageData['Organization']
      mainSr = villageData['State_Region']
      mainTsp = villageData['Township']
      print(f"org: {mainOrg}")
      print(f"SR: {mainSr}")
      print(f"TSP: {mainTsp}")
    
      
  sheetList['All_villages']['data'] = allVillagesDataTmp
  allVillagesDataTmp = None

  allProviderData = sheetList['All_provider']['data']
  allProviderDataTmp = {}
  for providerData in allProviderData:
    providerPostCode = providerData['Provider_Post_Code']
    if providerPostCode != '':
      del providerData['Provider_Post_Code']
      if not providerPostCode in allProviderDataTmp:
        allProviderDataTmp[providerPostCode] = {}

      personCode = providerData['Person_Code']
      # print("Checking " + personCode + " | " + providerPostCode);
      if personCode != '':
        del providerData['Person_Code']
        allProviderDataTmp[providerPostCode][personCode] = providerData
  sheetList['All_provider']['data'] = allProviderDataTmp
  allProviderDataTmp = None
#   print(json.dumps(sheetList['All_provider'],indent=4))


  tmpHeader = {}
  for header in headers:
    if header['Target sheet'] != 'End':
      if not header['Target sheet'] in tmpHeader:
        tmpHeader[header['Target sheet']] ={}
      tmpHeader[header['Target sheet']][str(header['Target column'])] = header['Heading text']
  headers = tmpHeader
  tmpHeader = None

  dRule = {}
  for dateRule in dateRules:
    sheet = dateRule['Target Sheet']
    targetColIndex = str(col_to_num(dateRule['Target column']))
    heading = headers[sheet][targetColIndex]
    if not sheet in dRule:
      dRule[sheet] = {}
    dateValue = ''
    try:
      dateValue = datetime.strptime(dateRule['value1'], '%-m/%-d/%Y')
    except:
      dateValue = datetime.strptime('01/01/2000', '%m/%d/%Y')
    
    dRule[sheet][heading] = {}
    dRule[sheet][heading]['list'] = [dateValue]

  nRule = {}
  for numRule in numRules:
    sheet = numRule['Target Sheet']
    targetColIndex = str(col_to_num(numRule['Target column']))
    heading = headers[sheet][targetColIndex]
    if not sheet in nRule:
      nRule[sheet] = {}
    value1 = 0.0
    value2 = 0.0
    try:
      value1 = float(numRule['value1'])
    except:
      value1 = 0.0
    
    try:
      value2 = float(numRule['value2'])
    except:
      value2 = 0.0
    nRule[sheet][heading] = {}
    nRule[sheet][heading]['list'] = [value1,value2]  
  
  rpFile = gc.open_by_url(url_of_report_file) 
  rpVar = rpFile.worksheet('var')
  rpVarx = rpVar.get_all_values()
  pdKey = rpVarx[0]
  pdVal = rpVarx[1:]
  df = pd.DataFrame(pdVal,columns=pdKey)
  df.replace('', pd.NA, inplace=True)
  rpVarVal = []
  for column_name, column_data in df.items():
      coldatax = column_data.dropna().to_list()
      coldata = [[coldataxx] for coldataxx in coldatax]
      rpVarVal.append(coldata)
  
  ddRule = {}
  for dvRule in dvRules:
    sheet = dvRule['Target Sheet']
    targetColIndex = str(col_to_num(dvRule['Target column']))
    heading = headers[sheet][targetColIndex]
    if not sheet in ddRule:
      ddRule[sheet] = {}
    ddRule[sheet][heading] = {}

    # ruleList = rpVar.get(dvRule["Rule column"] + str(2) + ":" + dvRule["Rule column"])
    ruleList = rpVarVal[col_to_num(dvRule['Rule column']) - 1]
    # print(ruleList)
    # print(ruleListLol)
    # print(dvRule['Target Sheet'] + " | " + heading)
    ddRule[dvRule['Target Sheet']][heading]['list'] = []
    for ruleItem in ruleList:
      ddRule[dvRule['Target Sheet']][heading]['list'].append(ruleItem[0])

  verifyFindingSheet.append_rows(checkDataValidation(verifyFindingSheet, mainOrg, mainSr, mainTsp,sheetList, ddRule, nRule, dRule))

  verifyFindingSheet.append_rows(checkAllVillagesSheet(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkAllProviderSheet(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkPMO(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkPLA(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkIpcAdditional(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkGhtWsHe(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkLlinDistMassCont(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkLlinAnc(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkLlinOther(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkRecruitment(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkC19(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkIecDist(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkCommodityDist(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkProcurement(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkCboSupport(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkDesignDevelop(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkStudyAssessmentSurvey(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkVisit(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkTMW(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkMSS(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkTrainingProvider(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkCsg(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkCsgSmallGrant(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkIcmvOtherDisease(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
  verifyFindingSheet.append_rows(checkPatientRecord(verifyFindingSheet, mainOrg, mainSr, mainTsp, sheetList))
