from pylab import rcParams
import sqlite3
import datetime
import time
import matplotlib.pyplot as plt
#http://pythonprogramming.net/sqlite-part-3-reading-database-python/
import pandas as pd
pd.options.display.mpl_style = 'default'
import numpy as np
conn = sqlite3.connect('/Users/Lucas/Downloads/Banco0.db')
c = conn.cursor()

def tableCreate():
    c.execute("CREATE TABLE stuffToPlot(ID INT, unix REAL, datestamp TEXT, keyword TEXT, value REAL)")


def dataEntry():
    c.execute("INSERT INTO stuffToPlot VALUES(1, 1365952181.288,'2013-04-14 10:09:41','Python Sentiment',5)")
    c.execute("INSERT INTO stuffToPlot VALUES(2, 1365952257.905,'2013-04-14 10:10:57','Python Sentiment',6)")
    c.execute("INSERT INTO stuffToPlot VALUES(3, 1365952264.123,'2013-04-14 10:11:04','Python Sentiment',4)")
    conn.commit()

idfordb = 4
keyword = 'Python Sentiment'
value = 7

def dataEntry2():
    date = str(datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S'))
    c.execute("INSERT INTO stuffToPlot (ID, unix, datestamp, keyword, value) VALUES (?, ?, ?, ?, ?)",
              (idfordb, time.time(), date, keyword, value))
    conn.commit()

def readData():
    sql = "SELECT * FROM stuffToPlot WHERE keyword =? AND source =?" #consulta p 2 variaveis
    wordUsed = 'Python Sentiment'
    sourceVariable = 'twitter'
    for row in c.execute(sql, [(wordUsed), (sourceVariable)]):
        print str(row).replace(')','').replace('(','').replace('u\'','').replace("'","")

def readAll():
    sql = "SELECT * FROM CONTAS"
    result = c.execute(sql)
    for i, row in enumerate(result):
        #print i
        print str(row).replace(", u'","'")

def readToDf():
    df = pd.read_sql_query("SELECT * from CONTAS", conn)
    #print df
    return df

def separaHeD(df):

    #print df.head()
    #print df.tail()
    #print df.index
    #print df.columns
    #print df.values
    #print df.sort_index(axis=1, ascending=False).head() #ordena os titulos das colunas
    print "Coluna ordenada"
    print df.sort(columns="PRECO").head()#ordena 1 coluna
    #print df["NOME"].tail() #pega 1 coluna
    #print df.iloc[3]#pega uma linha
    #print df.iloc[3:5,1:5]#linhas 1 a 3 e colunas 1 a 4
    #print df[df["PRECO"]>10].tail() #linhas com preco maior q 10

    #df.to_csv('/Users/Lucas/Downloads/Banco.csv')

    dfData = df["DATA"]
    df2 = df.loc[:,["ID","CODIGO_CONTA","NOME","PRECO"]]
    print "Data removida"
    print dfData.head()

    dataList = []
    horaList = []
    for row in dfData:
        [d, h] = row.split(" ",10)
        dataList.append(str(d))
        horaList.append(str(h))

    data = pd.Series(dataList)
    hora = pd.Series(horaList)

    print "Tamanho dos dataframes"
    print data.size
    print hora.size
    print df2["ID"].size

    df2 = pd.concat([df2,data,hora],axis=1)

    #               0      1      2       3      4      5
    df2.columns = ["ID","CONTA","NOME","PRECO","DATA","HORA"]
    print "Data e hora em colunas separadas"
    print df2.tail()
    #print "Dados"
    #print df2.describe()
    return df2

def readProducts():
    sql = "SELECT * FROM PRODUTOS"
    result = c.execute(sql)
    for i, row in enumerate(result):
            #print i
        print str(row).replace(", u'","'")


def graphs(df):
    plt.rc("font", size=8)
    print "Total Diario"
    print df.iloc[:,(3,4)].groupby("DATA").sum()
    df.iloc[:,(3,4)].groupby("DATA").sum().plot()
    plt.ylabel("R$")
    plt.title("Total Diario")
    plt.savefig("/Users/Lucas/Desktop/quinta/TotalDiario.pdf")
    plt.show()

    print "Total por conta"
    print df.iloc[:,(1,3)].groupby("CONTA").sum().plot()
    plt.ylabel("R$")
    plt.title("Total por conta paga")
    plt.savefig("/Users/Lucas/Desktop/quinta/TotalporConta.pdf")

    plt.show()

    print "Total de produto por dia:"
    print df.iloc[:,(2,3,4)].groupby(["NOME","DATA"]).sum()

    print "Total de Vendas de cada produto ate hj:"
    print df.iloc[:,(2,3,4)].groupby(["NOME"]).sum()

    print "Total de Vendas de cada produto depois ou antes de tal dia"
    print df[df["DATA"]>"10/09/2015"].iloc[:,(2,3,4)].groupby(["NOME"]).sum()

    #rcParams['figure.figsize'] = 5, 5

    df[df["DATA"]>"10/09/2015"].iloc[:,(2,3,4)].groupby(["NOME"]).sum().plot(kind="bar")
    plt.ylabel("R$")
    plt.title("Total de vendas")
    plt.subplots_adjust(bottom=0.28)
    plt.savefig("/Users/Lucas/Desktop/quinta/Total.pdf")
    plt.show()

    df[df["DATA"]>"10/09/2015"].iloc[:,(2,3,4)].groupby(["NOME"]).sum().plot(kind="pie", subplots=True)
    plt.ylabel("R$")
    plt.title("Total de vendas")
    plt.savefig("/Users/Lucas/Desktop/quinta/Total2.pdf")
    plt.show()
    return df


df = readToDf()
df = separaHeD(df)
print "---------------"
graphs(df)

