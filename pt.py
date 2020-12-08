from sqlparse import lexer
from sqlparse import sql, tokens as T

sql = '''
select distinct x, # as x
    y, # as y
    z as zz, # as z
    a as a # zzz
# hello
, *
into outfile "hello.txt" 
from a as a, # this is a
     b as b  # this is b
where a.a = b.a # yes, a == a
and b.a = 'hello'
group by 1 order   by 1 limit 10
'''
sql='''
select  MID, 파트너이름, ZID, 존이름, 블락시간,

(select sci.coupon_policy_id from soplus_contract_info sci where CID = sci.car_id and sci.state = 'ACTIVE') as 쿠폰정책,
(select discount_percent from coupon_policy where id = 쿠폰정책) as 할인율


from

(select
		ci.id as CID,
        if(ri.member_id = 1500,'사고블락','CS블락') as 블락,
		ci.owner_member_id as MID,
        (select name from member_info where id = MID) as 파트너이름,
        ri.id as RID,
        ri.zone_id  as ZID,
        (select name from carzone_info where id = ZID) as 존이름,
        ri.start_at as 블락시작,
        ri.end_at as 블락종료,
        timestampdiff(minute,ri.start_at,ri.end_at)/60/24 as 블락시간


from car_info ci, reservation_info ri
where ci.id = ri.car_id
	and ci.sharing_type = 'zplus'
    and ri.state = 2
    and ri.member_id in (1500, 648))temp
left join reservation_memo rm on rm.info_key = RID and rm.hello = 'world'
where rm.memo like '%사고%' and rm.state = 1
group by RID
order by 할인율
'''

encoding = None
stream = lexer.tokenize(sql, encoding)

def pre(stream):
    for ttype, value in stream:
        if ttype in T.Keyword:
            value = value.upper()
            yield ttype, value
        elif ttype in T.Comment.Single:
            continue
        elif ttype in T.Comment:
            continue
        elif ttype in T.Text.Whitespace:
            continue
        else:
            yield ttype, value

print(tuple(pre(stream)))