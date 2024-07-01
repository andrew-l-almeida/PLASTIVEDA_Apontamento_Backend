from sqlalchemy.engine import create_engine, URL
from sqlalchemy import text, Table, MetaData, select, and_
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

pd.options.display.max_rows = None

class Areco:
    def __init__(self):
        self.metadata = MetaData()
        
        self.create_engine(os.getenv('USER_ANDREW'), os.getenv('PASSWORD_ANDREW'))
        
        
    def get_order(self, cd_Ped):
        query = f""" select B.cdEnt as Empresa, 
A.id_ped, 
A.cdPedido, 
C.nome, 
A.StatusPedido, 
count(D.id_it_pedido) as TotalItens, 
format(A.dt_Pedido, 'yyyy-MM-dd HH:mm:ss') as dt_Pedido, 
format(A.dt_LimiteEntrega, 'yyyy-MM-dd HH:mm:ss') as dt_LimiteEntrega

from pedidos						A
inner join entidade					B on A.id_Empresa = B.id_ent
inner join entidade					C on A.id_Cliente = C.id_ent
inner join it_pedido				D on D.id_ped = A.id_ped
where A.pierSitReg = 'ATV' and A.statusPedido not in ('AGU', 'CAN', 'DIG') and D.pierSitReg = 'ATV' and A.cdPedido = :cd_Ped
group by B.cdEnt, A.id_ped, A.cdPedido, C.nome, A.statusPedido, A.dt_pedido, A.dt_limiteEntrega

order by A.cdPedido
        """
        
        result = pd.read_sql_query(query, self.engine, params={'cd_Ped': cd_Ped})
        
        return result.to_dict(orient='records')
    
    
    def get_orders(self):
        query = f""" select B.cdEnt as Empresa, 
A.id_ped, 
A.cdPedido, 
C.nome, 
A.StatusPedido, 
count(D.id_it_pedido) as TotalItens, 
format(A.dt_Pedido, 'yyyy-MM-dd HH:mm:ss') as dt_Pedido, 
format(A.dt_LimiteEntrega, 'yyyy-MM-dd HH:mm:ss') as dt_LimiteEntrega,
rtrim(A.PedidoCliente) as PedidoCliente

from pedidos						A
inner join entidade					B on A.id_Empresa = B.id_ent
inner join entidade					C on A.id_Cliente = C.id_ent
inner join it_pedido				D on D.id_ped = A.id_ped
where A.pierSitReg = 'ATV' and A.statusPedido not in ('AGU', 'CAN', 'DIG') and D.pierSitReg = 'ATV'
group by B.cdEnt, A.id_ped, A.cdPedido, C.nome, A.statusPedido, A.dt_pedido, A.dt_limiteEntrega, A.PedidoCliente

order by A.cdPedido
        """
        
        result = pd.read_sql_query(query, self.engine)
        
        return result.to_dict(orient='records')
    
    
    def get_order_details(self, id_pedido):
        
        stmt = """SELECT Empresa.cdEnt AS CodEmpresa
, Cliente.Nome AS NomeCliente
, format(Pedidos.dt_Pedido, 'd', 'en-US') AS dt_Pedido
, Pedidos.id_ped
, Pedidos.cdPedido AS cdPedido
, Rtrim(Pedidos.PedidoCliente) AS PedidoCliente
, ITEM.id_it_pedido
, ROW_NUMBER() OVER (PARTITION BY pedidos.Id_Ped ORDER BY item.id_it_pedido) AS Item
, Materiais.cd_Referencia AS cd_Referencia
, rtrim(Materiais.ds_Prod) AS ds_Prod
, rtrim(Marca.ds_marca_prod) as Setor
, rtrim(Grupo.ds_GrpProd) as Grupo
, rtrim(SubGrupo.ds_SubGrupoPrd) as SubGrupo
--, Item.PrecoUnitario * DBO.GetFatorConversao(Pedidos.id_Moeda,Moeda.Id_Moeda,Pedidos.dt_Pedido) AS PrecoUnitario
, Item.qtdItem AS qtdItem
, Item.SitItemPedido --CASE WHEN Item.SitItemPedido = 'LIQ' THEN 'Encerrado' ELSE CASE WHEN Item.SitItemPedido = 'CSL' THEN 'Aberto' ELSE CASE WHEN Item.SitItemPedido = 'CAN' THEN 'Cancelado' END END END AS SitItemPedido
--, CASE WHEN Item.SitItemPedido = 'LIQ' or Item.SitItemPedido = 'CAN' then 'Encerrado' else case when DATEDIFF(day, GETDATE(), ITEM.dt_LimiteEntrega) < 0 then 'Atrasado' else case when DATEDIFF(day, getdate(), ITEM.dt_LimiteEntrega) >=0 and DATEDIFF(day, GETDATE(), ITEM.dt_LimiteEntrega) <=7 then 'Vencimento Proximo' else case when DATEDIFF(day, getdate(), ITEM.dt_LimiteEntrega) > 7 then 'No Prazo' end end end end AS STATUS
, FORMAT(Item.dt_LimiteEntrega, 'yyyy-MM-dd HH:mm:ss') AS dt_LimiteEntrega
, Item.QtdSaldo AS QtdSaldo
, UnidMedidaVenda.cd_UnidMed AS UnidVenda
--, (Item.qtdItem * Item.PrecoUnitario) * DBO.GetFatorConversao(Pedidos.id_Moeda,Moeda.Id_Moeda,Pedidos.dt_Pedido) AS ValorTotal
, DAY(item.dt_LimiteEntrega) as DiaLimiteEntrega
, MONTH(item.dt_LimiteEntrega) as MesLimiteEntrega
, YEAR(item.dt_LimiteEntrega) as AnoLimiteEntrega
, unid_Medidas.cd_UnidMed
, isNull(Ordem.cd_of, 0) as OrdemFabricacao
, isNull(Ordem.StatusOF, 'NFD') as StatusOF
, isNull(Ordem.id_of, 0) as id_of
 FROM Pedidos AS Pedidos  WITH(NOLOCK) 
INNER JOIN Entidade AS Empresa  WITH(NOLOCK) ON (Empresa.Id_Ent = Pedidos.id_Empresa)
INNER JOIN Entidade AS Cliente  WITH(NOLOCK) ON (Cliente.Id_Ent = Pedidos.id_Cliente)
INNER JOIN It_Pedido AS Item  WITH(NOLOCK) ON (Item.id_Ped = Pedidos.id_Ped)
INNER JOIN Unid_Medidas AS UnidMedidaVenda  WITH(NOLOCK) ON (UnidMedidaVenda.id_unidMed = Item.id_unidMed)
INNER JOIN Materiais AS Materiais  WITH(NOLOCK) ON (Materiais.id_Produto = Item.id_Produto)
left join CatProd as Categoria with(nolock) on (Categoria.idCatProd = Materiais.idCatProd)
left join Marcas_Prod as Marca with(nolock) on (marca.id_marca_prod = Materiais.id_marca_prod)
left join SubGrupoProduto as SubGrupo with(nolock) on (SubGrupo.id_SubGrupoPrd = Materiais.id_SubGrupoPrd)
left join GrupoProduto as Grupo with(nolock) on (Grupo.id_grpProd = SubGrupo.id_grpProd)
inner join Unid_medidas as Unid_Medidas with (nolock) on (Unid_Medidas.id_unidMed = Item.id_unidMed)
left join RlcProgramacaoItPedido as RlcProgramacaoItPedido with (nolock) on (RlcProgramacaoItPedido.id_it_pedido = Item.id_it_pedido and RlcProgramacaoItPedido.PierSitReg = 'ATV')
left join ProgramacaoProducao as Programacao with (nolock) on (Programacao.id_ProgProdPCP = RlcProgramacaoItPedido.id_ProgProdPCP and Programacao.pierSitReg = 'ATV')
left join OrdemFabricacao as Ordem with (nolock) on (Ordem.id_of = Programacao.id_of and Ordem.PierSitReg = 'ATV')
WHERE (Pedidos.PierSitReg = 'ATV')
AND (Item.PierSitReg = 'ATV')
and (Pedidos.id_ped = ?)
        """
        
        params = (id_pedido, )

        result = pd.read_sql_query(stmt, self.engine, params=params)
        
        return result.to_dict(orient='records')
    
    def get_of_details(self, id_of):
        operations_stmt = """
        select A.id_of, 
B.id_LstProcOF, 
A.cd_of, 
B.Seq_operacao, 
C.id_operacao, 
rtrim(C.ds_operacao) as ds_operacao
from OrdemFabricacao					A
inner join LstProcOF					B on a.id_of = B.id_of
inner join OperacoesProdutivas			C on C.id_operacao = B.id_operacao
where A.pierSitReg = 'ATV' and a.id_of = ?
order by 1,4
        """
        params = (id_of, )
        operations = pd.read_sql_query(operations_stmt, self.engine, params=params)
        
        print(operations)
        
        records_stmt = """select A.id_of, 
B.id_LstProcOF, 
A.cd_of, 
B.Seq_operacao, 
C.id_operacao, 
C.ds_operacao,
isNull(E.QtdProduzida, 0) as QtdApontadaFabricacao,
isNull(E.QtdRefugo, 0) as QtdApontadaRefugo,
isNull(E.id_apontProd, 0) as id_apontProd,
format(E.dt_inicio, 'yyyy-MM-dd HH:mm:ss') as dt_Inicio,
format(E.Hora_Inicio, 'yyyy-MM-dd HH:mm:ss') as Hora_Inicio,
format(E.DataFim, 'yyyy-MM-dd HH:mm:ss') as DataFim,
format(E.HoraFim, 'yyyy-MM-dd HH:mm:ss') as HoraFim,
IsNull(E.id_Prof_Intern, 0) as id_Prof_Intern,
isNull(F.nome, '') as NomeApontador
from OrdemFabricacao					A
inner join LstProcOF					B on a.id_of = B.id_of
inner join OperacoesProdutivas			C on C.id_operacao = B.id_operacao
left join ApnProdTempoOnLine			E on E.id_lstProcOf = B.id_lstProcOf and E.id_of = B.id_of and E.pierSitReg = 'ATV'
left join entidade						F on F.id_ent = E.id_prof_intern
where A.pierSitReg = 'ATV'  and a.id_of = ?
order by 1,4
        """
        
        records = pd.read_sql_query(records_stmt, self.engine, params=params)
        
        return_array = []
        
        
        for index, row in operations.iterrows():
            records_array = []
            
            for recordsIndex, records_row in records.iterrows():
                if(records_row['id_LstProcOF'] == row['id_LstProcOF']):
                        record_dict = {
                            'id_apontProd': records_row['id_apontProd'],
                            'Seq_operacao': records_row['Seq_operacao'],
                            'ds_operacao': records_row['ds_operacao'],
                            'QtdApontadaFabricacao': records_row['QtdApontadaFabricacao'],
                            'QtdApontadaRefugo': records_row['QtdApontadaRefugo'],
                            'dt_Inicio': records_row['dt_Inicio'],
                            'Hora_Inicio': records_row['Hora_Inicio'],
                            'DataFim': records_row['DataFim'],
                            'HoraFim': records_row['HoraFim'],
                            'id_Prof_Intern': records_row['id_Prof_Intern'],
                            'NomeApontador': records_row['NomeApontador']
                        }
                        records_array.append(record_dict)
                        
            
            of_detail_dict = {
                'id_LstProcOf': row['id_LstProcOF'],
                'Seq_operacao': row['Seq_operacao'],
                'id_operacao': row['id_operacao'],
                'ds_operacao': row['ds_operacao'],
                'records': records_array
            }
            
            return_array.append(of_detail_dict)
        return return_array
        
        
    def create_engine(self, uid, pwd):
        connection_string = f"DRIVER={{{os.getenv('DRIVER')}}};SERVER={os.getenv('SERVER')};DATABASE={os.getenv('DATABASE')};UID={uid};PWD={pwd}"
        url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_string})
        self.engine = create_engine(url)
        
        
areco = Areco()
        
