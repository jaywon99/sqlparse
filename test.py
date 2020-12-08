import sys
import sqlparse
from sqlparse import engine
from sqlparse import filters
from sqlparse import sql as sql
from sqlparse import tokens as T
from sqlparse.utils import imt
import json
import re

# TODO: 결국에는 my own tokenzier가 될 듯
class MyFilter:
    def process(self, stream):
        for ttype, value in stream:
            if ttype in T.Keyword:
                value = re.sub(r'\s+', ' ', value.upper())
                yield ttype, value
            elif ttype in T.Comment:
                continue
            elif ttype in T.Text.Whitespace:
                continue
            else:
                yield ttype, value

def parse(stream, encoding=None):
    stack = engine.FilterStack()
    stack.enable_grouping()  # 이것을 믿을 수 있을까?
    stack.preprocess.append(MyFilter())
    return tuple(stack.run(stream, encoding))


def tokenizer(statement):
    for token in statement.tokens:
        yield token

## 여기까지

## TODO: LALR parser를 만들 수 있을까?
class Tokenizer:
    ''' sqlparser의 statement를 parameter로 받아서, tokens 단위로 return
    '''
    def __init__(self, statement):
        self._tokens = statement.tokens
        self._idx = 0

    def __iter__(self):
        self._idx = 0

    def __next__(self):
        if self._idx >= len(self._tokens):
            raise StopIteration

        token = self._tokens[self._idx]
        self._idx += 1
        return token

    def push_back(self):
        if self._idx == 0:
            raise IndexError

        self._idx -= 1

class Field:
    def __init__(self):
        self.name = None
        self.alias = None


class Table:
    def __init__(self):
        self.name = None
        self.alias = None
        self.ttype = None   # table, subquery

## Token related methods
def is_keyword(token, keyword):
    return token and token.is_keyword and token.value == keyword

## Token related methods
def is_keywords(token, keywords):
    return token and token.is_keyword and token.value in keywords

## Token related methods
def is_join(token):
    return token and token.is_keyword and \
        ((len(token.value) == 4 and token.value[-4:] == 'JOIN') or
         (len(token.value) > 4 and token.value[-5:] == ' JOIN'))


## Token related methods
def is_stopper(token, K=None, C=None):
    if token:
        if K:
            if token.is_keyword and token.value in K:
                return True
        if C:
            for c in C:
                if isinstance(token, C):
                    return True
    return False
                
class SQLParser:
    pass


class Select(SQLParser):
    def __init__(self):
        self._fields = []
        self._tables = []
        self._where = None
        self._having = None

    def parse_fields(self, tokens):
        # Parse (Identifier | IdentifierList) Punctuation (Identifier | IdentifierList)
        # and build single IdentifierList
        token = next(tokens)
        if is_keyword(token, 'DISTINCT'):
            # TODO: add distinct
            token = next(tokens)

        self._fields = sql.IdentifierList()
        # TODO: 여기를 Generator로 만든다.
        while not is_stopper(token, ('FROM', 'INTO')):
            if isinstance(token, sql.IdentifierList):
                self._fields.tokens.extend(token.tokens)  # remove Punctuation from IdentifierList
            elif token.ttype == T.Punctuation:
                pass
            else:
                self._fields.tokens.append(token)

            token = next(tokens)

        return token

    def parse_from(self, tokens):
        # Parse (Identifier | IdentifierList) Punctuation (Identifier | IdentifierList)
        # and build single IdentifierList
        token = next(tokens)

        self._tables = sql.IdentifierList()
        # TODO: 여기를 Generator로 만든다.
        while not is_stopper(token, K=('GROUP BY', 'ORDER BY', 'LIMIT', 'UNION', 'UNION ALL'), C=(sql.Where, sql.Having)):
            if isinstance(token, sql.Identifier):
                self._tables.tokens.append(token)
            elif isinstance(token, sql.IdentifierList):
                self._tables.tokens.extend(token.tokens)  # remove Punctuation from IdentifierList
            elif token.ttype == T.Punctuation:
                pass
            elif is_join(token):
                # JOIN을 class로 만들것
                id = next(tokens)  # table name
                token = next(tokens)  # table on/using/nothing
                if is_keyword(token, 'USING'):
                    self._tables.tokens.append((id, next(tokens)))
                elif isinstance(token, sql.On):
                    self._tables.tokens.append((id, token))
                else:
                    continue
            elif token.ttype == T.Keyword:
                self._tables.tokens.append(token)
            else:
                print("!!!!!!UNEXPECTED", token, token.ttype, token.value, token.__class__)

            token = next(tokens)

        # print("ENDING TOKEN", token, token.ttype, token.__class__)
        return token

    def parse_single(self, keyword, tokens):
        token = next(tokens)
        # print(keyword, "---", token.value)
        return next(tokens)

    def parse_select(self, tokens):
        # wait until select and show DEBUG message
        token = next(tokens)
        while not is_keyword(token, 'SELECT'):
            print(token)  # show debug message
            sys.exit(0)

        token = self.parse_fields(tokens)

        # PROCESS INTO options
        if is_keyword(token, 'INTO'):
            token = self.parse_single('INTO', tokens)  # guess only single identifier..

        # PROCESS FROM
        if is_keyword(token, 'FROM'):
            token = self.parse_from(tokens)

        if isinstance(token, sql.Where):
            self._where = token  # where is class itself
            token = next(tokens)

        if is_keyword(token, 'GROUP BY'):
            token = self.parse_single('GROUPBY', tokens)  # guess only single identifier..

        if isinstance(token, sql.Having):
            self._having = token  # where is class itself
            token = next(tokens)

        if is_keyword(token, 'ORDER BY'):
            token = self.parse_single('ORDERBY', tokens)  # guess only single identifier..

        if is_keyword(token, 'LIMIT'):
            token = self.parse_single('LIMIT', tokens)  # guess only single identifier..

        return token

    def parse(self, statement):
        tokens = tokenizer(statement)

        try:
            while True:
                token = self.parse_select(tokens)
                if is_keyword(token, 'UNION') or is_keyword(token, 'UNION ALL'):
                    # do again!
                    # TODO: make union/union all to anther Select class
                    continue
                break

            print("OOPS, SOME TOKENS LEFT", token, token.__class__)
            # sys.exit(0)
            while True:
                token = next(tokens)
                print("OOPS, SOME TOKENS LEFT", token, token.__class__)

        except StopIteration:
            pass

    @property
    def fields(self):
        return self._fields

    @property
    def tables(self):
        return self._tables


class SQLStatement:
    def __init__(self):
        pass

    def parse(self, statement):
        if statement.get_type() != 'SELECT':
            select = Select()
            select.parse()
            return select
        else:
            raise "Only Select Statement can be parsed."


# https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/sql.py
# ttype = Token Type
def navigate(obj, depth=0):
    # if obj.is_whitespace:
    #     return
    # print(obj.__class__)
    if obj.is_group:  # isinstance(obj, sql.TokenList):
        print(" ", " " * depth, obj.__class__)
        for t in tokenizer(obj):
            navigate(t, depth=depth + 2)
    else:
        print("*", " " * depth, obj.ttype, obj.value)


with open('../elasticsearch/query.json', 'r') as f:
    data = json.load(f)

# line = data[5791]['_source']['argument']
line = '''
select distinct x, # as x
    y, # as y
    a.z as zz, # as z
    a as a # zzz
# hello
, count(*)
into outfile "hello.txt" 
from a as a, # this is a
     b as b  # this is b
     left join schema_name on a.a=b.c
where a.a + a.b * a.c = b.a # yes, a == a
and a.b between a.c and a.d
and b.a = 'hello'
group by 1 order   by 1 limit 10
'''
line='''
SELECT
t2.member_id, t2.age, t2.sex, round(sum(t2.usetime),0) AS utime, COUNT(distinct t1.id) AS '친추', COUNT(DISTINCT t2.id) AS cnt,
(select count(a.id) from car_accident_info  a where a.member_id = t2.member_id and a.state not in (99) group by t2.member_id) as accident, ## 나매추가
(select count(m2.id) from member_memo m2 where m2.info_key = t2.member_id and m2.state = 2 group by t2.member_id) as cs ## 나매추가


FROM
(SELECT date(created_at) AS d, id, recommended_userid AS ru
FROM member_info
WHERE imaginary = 0 AND created_at BETWEEN '2020-11-06 00:00:00' AND '2020-11-23 00:00:00' AND recommended_userid IS NOT NULL
)t1,
(SELECT
r.id, r.member_id, m.userid, r.age, if(RIGHT(m.front_ssn,1)=1, 'M', 'F') AS sex, TIMESTAMPDIFF(MINUTE, r.start_at, r.return_at)/60 AS usetime
FROM
reservation_info r, member_info m
WHERE
r.member_id = m.id
AND r.return_at BETWEEN '2020-11-06 00:00:00' AND '2020-11-23 00:00:00'
AND r.way IN ('d2d_oneway', 'd2d_round', 'round', 'oneway')
AND r.state = 3
)t2,
mkt_agree_info a

WHERE
t1.ru = t2.userid
AND t2.member_id = a.member_id
AND a.state = 1
AND a.SMS = 'Y'
GROUP BY t2.member_id
HAVING t2.age BETWEEN 26 AND 39 AND  accident is not null and cs is null
ORDER BY cnt DESC
'''

print(line)
select = Select()
navigate(parse(line)[0])
select.parse(parse(line)[0])
for f in select.fields:
    print(f, f.ttype, f.__class__)
print('----')
for f in select.tables:
    if isinstance(f, tuple):
        print(f)
    else:
        print(f.ttype, f.__class__, f)
        if isinstance(f, sql.Identifier):
            print("====", f.get_alias())
            if len(f.tokens) > 1:
                print("====", f.tokens[-1])
sys.exit(0)

for idx, datum in enumerate(data):
    statements = datum['_source']['argument']
    parsed = parse(statements)

    print(idx, statements)
    for s in parsed:
        print("PROCESS", idx)
        # s.get_type()
        # process SELECT only
        select = Select()
        select.parse(s)
        # if s.get_type() == 'SELECT':
        #     navigate(s)
        #     print("-------------------------")
        print("-------------------------")
