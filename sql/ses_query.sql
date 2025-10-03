SELECT 
    h.DOCID AS "Document Number",
    h.BELNR_FI AS "Accounting Document Number",
    h.BUKRS AS "Company Code",
    l.OPT_VIM_1LOG_STATUS AS "Invoice Status",
    l.OPT_VIM_1LOG_FUNC_TEXT AS "Activity",
    l.OPT_VIM_1LOG_ACTUAL_ROLE AS "Actual Role",
    l.OPT_VIM_1LOG_ACTUAL_AGENT AS "Actual Agent",
    l.OPT_VIM_1LOG_START_DATE_TIME AS "Start Date & Time",
    l.OPT_VIM_1LOG_END_DATE_TIME AS "End Date & Time",
    l.OPT_VIM_1LOG_WORKITEM_ID AS "Work Item ID",
    t.PROC_TYPE AS "Process Type Number",
    t.OBJTXT AS "Process Type Text"
FROM
DSS.VIM_OPT_VIM_1LOG_VW l
JOIN 
DSS.VIM_1HEAD_2HEAD_VW h 
ON 
l.OPT_VIM_1LOG_DOCID = h.DOCID
JOIN
DSS.VIM_STG_T800T_VW t
ON
l.OPT_VIM_1LOG_PROCESS_TYPE = t.PROC_TYPE
WHERE
l.OPT_VIM_1LOG_FUNC_TEXT = 'Bypassed Rule -QH - Service Entry Requir'
ORDER BY
h.DOCID, l.OPT_VIM_1LOG_START_DATE_TIME


