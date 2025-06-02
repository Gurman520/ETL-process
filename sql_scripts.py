# SQL скрипты для получения данных из БД
query_get_patient = '''
select 
	p.keyid, 
	p.sex, 
	p.birthdate, 
	p.death_dat, 
	(select l.text from solution_med.lu l where l.keyid = p.social_status_id) as social_status,
	CASE 
       WHEN p.areanum_lu_id IS NULL THEN '0' 
       ELSE '1' 
    END
	 as prikrep, 
	(select l.text from solution_med.lu l where l.keyid = p.group_lu_id) as group_lu,
	(select l.text from solution_med.lu l where l.keyid = p.rh_lu_id) as rezus_lu
from solution_med.patient p
''' 

query_get_doctor = '''
select 
	d.keyid,
	d.positionid,
   	(select d2."text"  from solution_med.dep d2 where d2.keyid = d.depid),
   	(select text from solution_med.lu where keyid = d.staff_docdep_id),
	d.status
from solution_med.docdep d
where d.depid in (22, 25, 26, 27, 29) and d.positionid is not null
'''

query_get_dolznost = '''
select l.keyid, l."text"
from solution_med.lu l 
where l.tag = 22
'''

amb_query_get_diagn_pat = '''
SELECT pdc.keyid
            ,pdc.patient_id
            ,CASE
               WHEN pdc.ill_type = 1 THEN
                'Острый'
               WHEN pdc.ill_type = 2 THEN
                'Хронический'
               ELSE
                'Без указания'
             END AS ill_type
            ,CASE
               WHEN pdc.disp_status = 1 THEN
                'Состоит'
               WHEN pdc.disp_status = 2 THEN
                'Не состоит'
               ELSE
                'Неизвестно'
             END AS disp_status
            ,d.code AS diag_code
            ,d.text AS diag_text
            ,to_char(pdc.reg_dat, 'dd.mm.yyyy') AS reg_dat
            ,pdc.reg_docdep_id AS reg_by
            ,to_char(pdc.confirm_dat, 'dd.mm.yyyy') AS confirm_dat
            ,pdc.confirm_docdep_id AS confirm_by
            ,to_char(pdc.end_dat, 'dd.mm.yyyy') AS end_dat
            ,pf_docdep.get_text(pdc.end_docdep_id) AS end_by
        FROM patdiag_confirm pdc
        JOIN diagnos d
          ON d.keyid = pdc.diagnos_id
       ORDER BY pdc.ill_type desc ,pdc.reg_dat
'''

amb_query_get_visit = '''
select v.keyid, v.patientid, v.num, v.dat, v.agrid, v.doctorid, d.code as diagnoz, (select d."text" from solution_med.dep d where d.keyid = v.depid )
from solution_med.visit v 
join solution_med.patdiag p on v.keyid = p.visitid
join solution_med.diagnos d on p.diagid = d.keyid 
where v.vistype < 100
'''

stac_query_get_visit = '''
SELECT 
	v.rootid AS visitID, 
	v.patientid,
	v.num AS num_b, 
	(SELECT a.text
		FROM solution_med.agr a 
		WHERE a.keyid = v.agrid),
	v.doctorid, 
	di.code as diagnoz,
	(select text FROM solution_med.dep WHERE keyid = v.dep1id) as ishod,
	v.dat,
	v.dat1,
	DATE(v.dat1) - DATE(v.dat) AS count_day,
	(select text FROM solution_med.dep WHERE keyid = d.keyid) AS department,
	(	SELECT 
		(SELECT pf_lu.get_text(sh.type_id)
        FROM serv_ht sh
        WHERE sh.keyid = coalesce(k.serv_ht_id, hp.serv_ht_id)) AS vmp_type
  	FROM kvota k
  	LEFT JOIN ht_profoper hp ON hp.keyid = k.ht_profoperid
  	LEFT JOIN kvt_opers ko ON ko.kvotaid = k.keyid
 	WHERE k.visitid2 = v.rootid) as vmp
FROM solution_med.visit v
JOIN solution_med.dep d ON v.depid = d.keyid
join solution_med.patdiag p on p.visitid = v.keyid
join solution_med.diagnos di on p.diagid = di.keyid
where p.diagtype in (1) and v.dep1id IN (SELECT keyid FROM solution_med.dep WHERE out_status=1 AND status=1) AND v.vistype IN (102, 103, 104, 107) 
'''

stac_query_get_PO_visit = '''
select 
	v.keyid as id, 
	v.dat as DAT_ST,
	v.dat1 as DAT_FIN,
	case
		when d.status_dep in (200, 202, 203, 205, 209, 210) then 'Отказ от госпитализации'
		else d."text"
	end as depgosp,
	case 
		when d.status_dep in (200, 202, 203, 205, 209, 210) then d."text"
		else 'Госпитализация'
	end as result,
	v.patientid AS PAT,
	dig.code as giag_code,
	dig."text" as diag_text,
	pkg_protocol_universal.get_answer(v.keyid, 'PLAN_HOSP', 'RegReception', 'text', 'REGIN') as form_help,
	pkg_protocol_universal.get_answer(v.keyid, 'WHOSENT', 'RegReception', 'text', 'REGIN') as who
from solution_med.visit v 
join solution_med.dep d on v.dep1id = d.keyid
join solution_med.patdiag p on p.visitid = v.keyid
join solution_med.diagnos dig on dig.keyid = p.diagid
where v.vistype = 101 and p.diagtype = 1
'''