#/usr/bin/python3
from fileinput import filename
from datetime import date, timedelta
import base64
import json
import requests
#modulos internos
import consts
import funcs as fcs
import ucontact as uc



def run():

    screen1() #EN PROCESO CASI COMPLETADO
    screen2() #LISTO!
    # screen3() # NO REALIZADO
    screen4() # NO REALIZADO

def screen1():
    # Time to contact (avg in minutes for each last 7 days)
    # NOTA: TOMAR EN CUENTA QUE INGRESA POR FRESHLEAD  Y DEBERIA CONTACTARSE SOBRE EL MISMO DIA, SEGUIR CON EL PLAN INICIAL DE INSERCION DE DATOS POR PARTE DE 
    # SQL SERVER Y HACER MERGE SOBRE LOANNUMBER.

    date_resp = fcs.get_date_range(7)
    
    query = """
    SELECT ROUND(AVG(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(cdr.parandvalues,":",3), ":", -1),"=",-1))) AS "AverageInDays", 
    CONCAT(MONTHNAME(cdr.calldate)," ",DAY(cdr.calldate)) AS "Date", cdr.parandvalues
    FROM ccrepo.cdr_repo cdr
    WHERE cdr.calldate >= '{} 00:00:00' AND cdr.calldate <= '{} 00:00:00'
    AND cdr.campaign IN ("LOANS<-", "LOANPredictive<-")
    AND cdr.peeraccount NOT IN ('PowerDialer', 'Predictive')
    AND cdr.parandvalues IS NOT null 
	 AND cdr.peeraccount IS NOT NULL
	 AND SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(cdr.parandvalues,":",4), ":", -1),"=",-1) IS NOT NULL
	 AND CHAR_LENGTH(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(cdr.parandvalues,":",3), ":", -1),"=",-1)) < 2
    GROUP BY DAY(cdr.calldate)
    ORDER BY cdr.calldate
    """.format(date_resp["day"], date_resp["today"])

    fcs.push_data_to_dashboard(query, consts._repo_dsn, "Date", "AverageInDays",  consts._file_name, "screen1", "timeToContact")
    
def screen2():
    date_resp = fcs.get_date_range(7)
     # Productivity
    query = """
            SELECT agent, ROUND(IFNULL((totalOccupation * 100) / totalLogin, 0)) Production FROM (
            SELECT agent, 
            SUM(IF (obj->'$.logged' > 900, 900, obj->'$.logged')) AS totalLogin, 
            SUM(IF (obj->'$.occupation' > 900, 900, obj->'$.occupation')) AS  totalOccupation
            FROM agent_stats 
            WHERE DATE >= "{} 00:00:00" AND DATE <= "{} 23:59:59" 
            AND obj->'$.logged' > 0
            GROUP BY agent ORDER BY DATE DESC) AS q
            ORDER BY Production DESC
            """.format(date_resp["day"], date_resp["today"])
    
    fcs.push_data_to_dashboard(query, consts._repo_dsn, "agent", "Production",  consts._file_name, "screen2", "productivity")
    
    # Production
    # vw_Loans: Originationdate. Por columna > Desde una semana hasta el dia presente la cantidad de loans.
    query = """
            SELECT COUNT(*) AS "LoansCount", CONCAT(DATENAME(mm, Originationdate),' ',DAY(Originationdate)) AS "DateNames"
            FROM vw_Loans 
            WHERE Originationdate 
            BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
            GROUP BY DAY(Originationdate), CONCAT(DATENAME(mm, Originationdate),' ',DAY(Originationdate))
            ORDER BY DAY(Originationdate)
            """.format(date_resp["day"], date_resp["today"])

    fcs.push_data_to_dashboard(query, consts._epic_dsn, "DateNames", "LoansCount",  consts._file_name, "screen2", "production")
    
def screen3():
    # Performance per Call Center (LCO/St.Louis)
    # No sabemos como definir ni su performance ni de donde sacamos estos datos.
    # No se puede saber nada segun lo que vimos con Paulo ya que el departamento no fue definido.
    pass

def screen4():
    #Portfolio Data / Loan processing
    #-------------------------------------------------------------

    date_resp = fcs.get_date_range(7)

    queries = {}
    #- Number of loans outstanding: new loan, pending collection, pending pay off,item pending pay off. Returned item pending paid off, returned item. / TABLA vw_LOANS: LoanStatus. 
    queries["number_of_loans_outstanding"] = """
    SELECT COUNT(*) AS "count", CAST(EffectiveDate AS Date ) AS "date" FROM dbo.vw_LOANS
    WHERE EffectiveDate IS NOT NULL
    AND EffectiveDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:00.000'
    AND LoanStatus IN ('New Loan', 'Pending Collection', 'Pending Paid Off', 'Returned Item Pending Paid Off', 'Returned Item')
    GROUP BY CAST(EffectiveDate AS Date )
    ORDER BY CAST(EffectiveDate AS Date )
    """.format(date_resp["day"], date_resp["today"])

    fcs.push_data_to_dashboard(queries["number_of_loans_outstanding"], consts._epic_dsn, "date", "count",  consts._file_name, "screen4", "number_of_loans_outstanding")
    
    
    #- Dollar outstanding: Suma de todos los loans outstandings que tenemos arriba en el principal. / Tabla vw_LoanHistory: OutstandingPrinciple.
    queries["dollar_outstandig"]="""
    SELECT SUM(OutstandingPrinciple) AS "total", CAST(EffectiveDate AS Date ) AS "date" FROM dbo.vw_LoanHistory
    WHERE EffectiveDate  IS NOT NULL
    AND EffectiveDate  BETWEEN '{} 00:00:00.000' AND '{} 23:59:00.000'
    AND LoanStatus IN ('New Loan', 'Pending Collection', 'Pending Paid Off', 'Returned Item Pending Paid Off', 'Returned Item')
    GROUP BY CAST(EffectiveDate  AS Date )
    ORDER BY CAST(EffectiveDate  AS Date )
    """.format(date_resp["day"], date_resp["today"])

    fcs.push_data_to_dashboard(queries["dollar_outstandig"], consts._epic_dsn, "date", "total",  consts._file_name, "screen4", "dollar_outstanding")

    #- Retornados en cada uno de esos 3 meses. VISTA vw_Payments: EffectiveDate. Sumar todos la cantidad de pagos realizados en el mes (por effectiveDate), 
    #   Sumar todos los pagos que fueron returned en el mismo mes (ReturnDate) y hacer la diferencia entre ambos. ????? 
    # PENDIENTE - ¿Tomo todos los EffectiveDate como 100% y todos los ReturnDate para hacer regla de 3?
    
    # Tenemos pagos que son por el banco "ACH"  y nos revisamos el PaymentStatus PARA TENER EN CUENTA EL MISMO COMO "REJECTED" Y "NONE".

    #- Charge off percent for each previews 3 months. VISTA vw_Loans: CollectionStartDate.
    # PENDIENTE - ¿PORCENTAJE DE QUE COSA?

    # COMO CALCULAR ESTO: 90 días atrás, tomar en cuenta el LOanStatus como "Charge Off" y la fecha por medio de CollectionStartDate 
    # ACLARACION IMPORTANTE> SOLO PARA ELOAN FUNCIONA ESTA FORMULA PUESTO QUE ELLOS DEFINEN CUANDO SERA EL CHARGE OFF.
    
    ## Un poco imposible que el Charge Off se pueda calcular, debemos consultar a Eloans que quiere especificamente
    #queries["chargeoff_percent"]="""
    #SELECT SUM(OutstandingPrinciple) AS "total", CAST(EffectiveDate AS Date ) AS "date" FROM dbo.vw_LoanHistory
    #WHERE CollectionStartDate  IS NOT NULL
    #AND CollectionStartDate  BETWEEN '{} 00:00:00.000' AND '{} 23:59:00.000'
    #AND LoanStatus IN ('New Loan', 'Pending Collection', 'Pending Paid Off', 'Returned Item Pending Paid Off', 'Returned Item')
    #GROUP BY CAST(CollectionStartDate  AS Date )
    #ORDER BY CAST(CollectionStartDate  AS Date )
    #""".format(date_resp["day"], date_resp["today"])


    #fcs.push_data_to_dashboard(queries["chargeoff_percent"], consts._epic_dsn, "date", "total",  consts._file_name, "screen4", "chargeoff_percent")
    


    #- Return customer count for each previews 3 months. VISTA vw_Loans: CollectionStartDate
    # Calcular todos los collections con fecha filtrada por CollectionStartDate y LoanStatus en ("Charge off", "Returned Item") menos 90 días
    

    #- Number of new customers for each previews 3 months. VISTA vw_Client: DateCreated. 
    date_resp = fcs.get_date_range(90)
    queries["newcustomers"]="""
    SELECT 'New customers count' as "total", COUNT(*) as "newCustomersCount" FROM dbo.vw_Client as "client"
    WHERE client.DateCreated BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    """.format(date_resp["day"], date_resp["today"])


    fcs.push_data_to_dashboard(queries["newcustomers"], consts._epic_dsn, "total", "newCustomersCount",  consts._file_name, "screen4", "newcustomers")

    
    #- Number of new customers on store 1 (ST01-LeadBuy-ELW) in 3 months. Hacer un merge entre VISTA vw_Loans: StoreNames, DebtorCliendId - vw_Client: id.
    date_resp = fcs.get_date_range(90)
    queries["newcustomers_st01"]="""
    SELECT 'New customers count' as "total", COUNT(*) as "newCustomersCount" FROM dbo.vw_Loans as "loans"
    INNER JOIN dbo.vw_Client as "client" ON loans.DebtorClientId = client.id
    WHERE loans.StoreName LIKE ' ST01-LeadBuy-ELW'
    AND client.DateCreated BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    """.format(date_resp["day"], date_resp["today"])


    fcs.push_data_to_dashboard(queries["newcustomers_st01"], consts._epic_dsn, "total", "newCustomersCount",  consts._file_name, "screen4", "newcustomers_st01")


    #- Number of new customers on store 2 (ST02) in 3 months. VISTA vw_Loans: StoreNames, DebtorCliendId - vw_Client: id.
    date_resp = fcs.get_date_range(90)
    queries["newcustomers_st02"]="""
    SELECT 'New customers count' as total, COUNT(*) as newCustomersCount FROM dbo.vw_Loans as loans
    INNER JOIN dbo.vw_Client as client ON loans.DebtorClientId = client.id
    WHERE loans.StoreName LIKE 'ST02-Organic-ELW'
    AND client.DateCreated BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    """.format(date_resp["day"], date_resp["today"])


    fcs.push_data_to_dashboard(queries["newcustomers_st02"], consts._epic_dsn, "total", "newCustomersCount",  consts._file_name, "screen4", "newcustomers_st02")


    #- Number of Loans past due (effective date) per 1 - 10, 30 days, 60 days, mas de 90 days:  VISTA vw_Loans: ChargeOffStartDate. 
    # (PARA CASO GENERICO, TOMAR EN CUENTA CollectionStartDate).
    date_10 = fcs.get_date_range(10)
    date_30 = fcs.get_date_range(30)
    date_60 = fcs.get_date_range(60)
    date_90 = fcs.get_date_range(90)

    queries["number_of_loans_past_due"]="""
    SELECT '1 - 10 days' as "title", COUNT(*) AS "NumberOfLoansPastDue" FROM dbo.vw_Loans
    WHERE CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    UNION 
    SELECT '1 - 30 days' as "title", COUNT(*) AS "NumberOfLoansPastDue" FROM dbo.vw_Loans
    WHERE CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    UNION 
    SELECT '1 - 60 days' as "title", COUNT(*) AS "NumberOfLoansPastDue" FROM dbo.vw_Loans
    WHERE CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    UNION 
    SELECT '90 days' as "title", COUNT(*) AS "NumberOfLoansPastDue" FROM dbo.vw_Loans
    WHERE CollectionStartDate >= '{} 00:00:00.000'
    """.format(
    date_10["day"], date_10["today"], 
    date_30["day"], date_30["today"],
    date_60["day"], date_60["today"],
    date_90["day"]
    )
    fcs.push_data_to_dashboard(queries["number_of_loans_past_due"], consts._epic_dsn, "title", "NumberOfLoansPastDue",  consts._file_name, "screen4", "number_of_loans_past_due")
    

    #- Dollars in past due per 1 - 10, 30 days, 60 days, mas de 90 days.
    queries["dollars_in_past_due"]="""
    SELECT '$ Past Due 10 days' as title, SUM(loanhistory.OutstandingTotal) AS "OutstandingTotal" FROM vw_LoanHistory AS "loanhistory"
    INNER JOIN vw_Loans AS "loans" ON loans.id = loanhistory.LoanId
    WHERE loans.CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'

    UNION

    SELECT '$ Past Due 30 days' as title, SUM(loanhistory.OutstandingTotal) AS "OutstandingTotal" FROM vw_LoanHistory AS "loanhistory"
    INNER JOIN vw_Loans AS "loans" ON loans.id = loanhistory.LoanId
    WHERE loans.CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'

    UNION

    SELECT '$ Past Due 60 days' as title, SUM(loanhistory.OutstandingTotal) AS "OutstandingTotal" FROM vw_LoanHistory AS "loanhistory"
    INNER JOIN vw_Loans AS "loans" ON loans.id = loanhistory.LoanId
    WHERE loans.CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'

    UNION

    SELECT '$ Past Due 90 days' as title, SUM(loanhistory.OutstandingTotal) AS "OutstandingTotal" FROM vw_LoanHistory AS "loanhistory"
    INNER JOIN vw_Loans AS "loans" ON loans.id = loanhistory.LoanId
    WHERE loans.CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000'
    """.format(
    date_10["day"], date_10["today"], 
    date_30["day"], date_30["today"],
    date_60["day"], date_60["today"],
    date_90["day"], date_90["today"]

    )


    fcs.push_data_to_dashboard(queries["dollars_in_past_due"], consts._epic_dsn, "title", "OutstandingTotal",  consts._file_name, "screen4", "dollars_in_past_due")
    
    #-Returned Percent = number returned vs (Success + Returned) for each last 3 months.
    queries["returned_percent"]="""
    SELECT 'Percent of rejected' AS title, (("Rejected" * 100) / "SuccessAndRejected" ) AS "PercentOfRejected" FROM(
    SELECT 
    sum(case when PaymentStatus = 'Checked' OR PaymentStatus = 'Rejected' OR  PaymentStatus = 'None' then 1 else 0 end)  AS "SuccessAndRejected",
    sum(case when PaymentStatus = 'Rejected' OR  PaymentStatus = 'None' then 1 else 0 end) "Rejected"
    FROM vw_Payments
    WHERE EffectiveDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000') q
    """.format(date_90["day"], date_90["today"])


    fcs.push_data_to_dashboard(queries["returned_percent"], consts._epic_dsn, "title", "PercentOfRejected",  consts._file_name, "screen4", "returned_percent")

    #- Charge off percent for each last 3 months.

    queries["charged_off_percent"]="""
    SELECT 'Percent of Charged Off' AS title, (("ChargedOff" * 100) / "ChargedOffAndNotChargedOff") AS "PercentOfChargedOff" FROM(
    SELECT 
    sum(case when LoanStatus != '' then 1 else 0 end) AS "ChargedOffAndNotChargedOff",
    sum(case when LoanStatus = 'Returned Item' OR  LoanStatus = 'Returned Item Paid Off' OR LoanStatus = 'Charged Off' then 1 else 0 end)  AS "ChargedOff" 
    FROM vw_Loans
    WHERE CollectionStartDate BETWEEN '{} 00:00:00.000' AND '{} 23:59:59.000') as q
    """.format(date_90["day"], date_90["today"])

    fcs.push_data_to_dashboard(queries["charged_off_percent"], consts._epic_dsn, "title", "PercentOfChargedOff",  consts._file_name, "screen4", "charged_off_percent")


    # - FPD .. First payment default (3 months).
    queries["fpd"]="""
    SELECT 'FPD' AS "title", COUNT(*) AS "Counter" FROM(
    SELECT pm.LoanId, MIN(pm.EffectiveDate) AS "EffectiveDate"
    FROM vw_Payments AS "pm"
    WHERE pm.isDebit != 0 AND pm.PaymentStatus != 'Checked'
    GROUP BY pm.LoanId
    ) q JOIN (
    SELECT pm.LoanId, MIN(pm.EffectiveDate) AS "EffectiveDate"
    FROM vw_Payments AS "pm"
    WHERE pm.isDebit != 0 
    GROUP BY pm.LoanId
    ) qq ON 
    qq.LoanId = q.LoanId AND qq.EffectiveDate = q.EffectiveDate
    AND q.EffectiveDate BETWEEN '{} 00:00:00' AND '{} 23:59:59'
    """.format(date_90["day"], date_90["today"])
    
    fcs.push_data_to_dashboard(queries["fpd"], consts._epic_dsn, "title", "Counter",  consts._file_name, "screen4", "fpd")

    # - Second payment default (3 months).

    queries["spd"]="""    
    SELECT 'SPD' AS "title", SUM(CASE WHEN PaymentStatus != 'Checked' then 1 else 0 END) AS "Counter" FROM (
    SELECT pm.LoanId, MIN(pm.EffectiveDate) AS "EffectiveDate", pm.PaymentStatus
    FROM vw_Payments AS "pm"
    WHERE pm.isDebit != 0  AND 
    pm.EffectiveDate > 
        (
        SELECT MIN(pmm.EffectiveDate) AS "EffectiveDate"
        FROM vw_Payments AS "pmm"
        WHERE pmm.isDebit != 0
        AND pmm.LoanId = pm.LoanId
        GROUP BY pmm.LoanId
        )
    GROUP BY pm.LoanId, pm.PaymentStatus) q
    WHERE q.EffectiveDate BETWEEN '{} 00:00:00' AND '{} 23:59:59'
    """.format(date_90["day"], date_90["today"])

    fcs.push_data_to_dashboard(queries["spd"], consts._epic_dsn, "title", "Counter",  consts._file_name, "screen4", "spd")

    # - TPD (3 months).
    queries["tpd"]="""
    
    SELECT 'TPD' AS "title", SUM(CASE WHEN PaymentStatus != 'Checked' then 1 else 0 END) AS "Counter" FROM (

    SELECT pp.LoanId, MIN(pp.EffectiveDate) AS "EffectiveDate", pp.PaymentStatus FROM vw_Payments AS "pp" 
    WHERE pp.isDebit != 0  AND 
    pp.EffectiveDate > 
        (
        SELECT MIN(pm.EffectiveDate) AS "EffectiveDate"
        FROM vw_Payments AS "pm"
        WHERE pm.isDebit != 0  AND 
        pm.EffectiveDate > 
            (
            SELECT MIN(pmm.EffectiveDate) AS "EffectiveDate"
            FROM vw_Payments AS "pmm"
            WHERE pmm.isDebit != 0
            AND pmm.LoanId = pm.LoanId
            GROUP BY pmm.LoanId
            )
        AND pm.LoanId = pp.LoanId
        GROUP BY pm.LoanId
        )
    GROUP BY pp.LoanId, pp.PaymentStatus
    ) q

   WHERE q.EffectiveDate BETWEEN '{} 00:00:00' AND '{} 23:59:59'
    """.format(date_90["day"], date_90["today"])

    fcs.push_data_to_dashboard(queries["tpd"], consts._epic_dsn, "title", "Counter",  consts._file_name, "screen4", "tpd")

if __name__ == "__main__":
    run()