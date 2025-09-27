import re
from graphviz import Digraph

sql = open('create_tables.sql').read()
dot = Digraph(format='png')
dot.attr(rankdir='LR', nodesep='1.5', ranksep='2.0', fontsize='12')

for t_name, body in re.findall(r'CREATE TABLE (\w+)\s*\((.*?)\);', sql, flags=re.S|re.I):
    cols, pk = [], None
    for line in body.split(','):
        line = line.strip()
        m = re.search(r'(\w+)\s+.*PRIMARY KEY', line, re.I)
        if m: pk = m.group(1)
        cols.append(line.split()[0])
    # Vytvor HTML-like tabuľku s portmi
    lbl = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="6"><TR><TD BGCOLOR="lightblue"><B>{t_name}</B></TD></TR>'
    lbl += ''.join(f'<TR><TD PORT="{c}">{"🔑 " if c==pk else ""}{c}</TD></TR>' for c in cols) + '</TABLE>>'
    dot.node(t_name, label=lbl, shape='plain')

# Znova pre foreign keys, tentokrát smerujú na konkrétny port
for t_name, body in re.findall(r'CREATE TABLE (\w+)\s*\((.*?)\);', sql, flags=re.S|re.I):
    for line in body.split(','):
        fk = re.search(r'(\w+)\s+\w+.*REFERENCES\s+(\w+)\s*\((\w+)\)', line, re.I)
        if fk:
            col, ref_table, ref_col = fk.groups()
            dot.edge(f'{t_name}:{col}', f'{ref_table}:{ref_col}', arrowhead='crow', color='blue')

dot.render('er_from_sql_columns', cleanup=True)
print("ER diagram uložený ako 'er_from_sql_columns.png'")
