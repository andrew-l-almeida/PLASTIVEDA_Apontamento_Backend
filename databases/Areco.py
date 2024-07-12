from sqlalchemy.engine import create_engine, URL
from sqlalchemy import text, MetaData
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

order by A.cdPedido desc
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
, isNull(Programacao.id_CtrlProjPCP, 0) as id_CtrlProjPCP 
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
rtrim(C.ds_operacao) as ds_operacao,
B.id_CtrlProjPCP,
B.UltProc
from OrdemFabricacao					A
inner join LstProcOF					B on a.id_of = B.id_of
inner join OperacoesProdutivas			C on C.id_operacao = B.id_operacao
where A.pierSitReg = 'ATV' and a.id_of = ?
order by 1,4
        """
        params = (id_of, )
        operations = pd.read_sql_query(operations_stmt, self.engine, params=params)
        
        records_stmt = """select A.id_of, 
B.id_LstProcOF, 
A.cd_of, 
B.Seq_operacao, 
C.id_operacao, 
rtrim(C.ds_operacao) as ds_operacao,
isNull(E.QtdProduzida, 0) as QtdApontadaFabricacao,
isNull(E.QtdRefugo, 0) as QtdApontadaRefugo,
isNull(E.id_apontProd, 0) as id_apontProd,
format(E.dt_inicio, 'yyyy-MM-dd HH:mm:ss') as dt_Inicio,
format(E.Hora_Inicio, 'yyyy-MM-dd HH:mm:ss') as Hora_Inicio,
format(E.DataFim, 'yyyy-MM-dd HH:mm:ss') as DataFim,
format(E.HoraFim, 'yyyy-MM-dd HH:mm:ss') as HoraFim,
IsNull(E.id_Prof_Intern, 0) as id_Prof_Intern,
isNull(F.nome, '') as NomeApontador,
isNull(G.id_maquina, 0) as id_Maquina,
rtrim(G.ds_maquina) as ds_Maquina
from OrdemFabricacao					A
inner join LstProcOF					B on a.id_of = B.id_of
inner join OperacoesProdutivas			C on C.id_operacao = B.id_operacao
left join ApnProdTempoOnLine			E on E.id_lstProcOf = B.id_lstProcOf and E.id_of = B.id_of and E.pierSitReg = 'ATV'
left join entidade						F on F.id_ent = E.id_prof_intern
left join maquinas						G on G.id_maquina = E.id_maquina
where A.pierSitReg = 'ATV'  and a.id_of = ?
order by 1,4
        """
        
        records = pd.read_sql_query(records_stmt, self.engine, params=params)
        
        return_array = []
        
        
        for index, row in operations.iterrows():
            records_array = []
            
            for recordsIndex, records_row in records.iterrows():
                record_dict = {}
                if(records_row['id_LstProcOF'] == row['id_LstProcOF']):
                        if records_row['id_apontProd'] != 0:
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
                                'NomeApontador': records_row['NomeApontador'],
                                'id_maquina': records_row['id_Maquina'],
                                'ds_maquina': records_row['ds_Maquina']
                            }
                            records_array.append(record_dict)
                        
            
            of_detail_dict = {
                'id_of': row['id_of'],
                'id_LstProcOf': row['id_LstProcOF'],
                'Seq_operacao': row['Seq_operacao'],
                'id_operacao': row['id_operacao'],
                'ds_operacao': row['ds_operacao'],
                'id_CtrlProjPCP': row['id_CtrlProjPCP'],
                'ultProc': row['UltProc'],
                'records': records_array
            }
            
            return_array.append(of_detail_dict)
        return return_array
    
    def get_operators(self):
        
        stmt = """SELECT Entidade.id_ent as id_Ent  
, Entidade.cdEnt AS cdEnt
, rtrim(Entidade.Nome) AS Nome
, Prof_Internos.id_Dept AS id_Dept
, rtrim(Departamento.dsDept) AS dsDept
, isNull(rtrim(CP.ds_celulaproducao), '') as CCusto
, Prof_Internos.id_Prof_Intern
 FROM Entidade AS Entidade  WITH(NOLOCK) 
INNER JOIN RlcEntidadeTipo AS RlcEntidadeTipo  WITH(NOLOCK) ON (RlcEntidadeTipo.Id_Ent = Entidade.Id_Ent)
INNER JOIN Tipo_Entid AS Tipo_Entid  WITH(NOLOCK) ON (Tipo_Entid.Id_Class = RlcEntidadeTipo.Id_Class)
INNER JOIN Prof_Internos AS Prof_Internos  WITH(NOLOCK) ON (Prof_Internos.Id_Ent = Entidade.Id_Ent)
INNER JOIN Departamento AS Departamento  WITH(NOLOCK) ON (Departamento.id_Dept = Prof_Internos.id_Dept)
left join PlanoCusteio	as PC with(nolock) on (PC.id_CCusto = Prof_Internos.id_CCusto)
left join CelulaProducao as CP with(nolock) on (cp.id_CCusto = PC.id_CCusto)
WHERE (Entidade.PierSitReg = 'ATV')
 AND (Tipo_Entid.cdClass = 4)
 AND (Prof_Internos.id_Dept = 8)
"""
    
        result = pd.read_sql_query(stmt, self.engine)
        
        setores = result['CCusto'].unique()
        
        return_array = []
        
        
        for setor in setores:
            department = {}
            
            department[setor] = []
            
            for index, row in result.iterrows():
                
                s = row['CCusto']
                nome = row['Nome']
                id_prof_intern = row['id_Prof_Intern']
                id_ent = row['id_Ent'] 
                
                newPerson = {
                    'nome': nome,
                    'id_prof_intern': id_prof_intern,
                    'id_ent': id_ent
                }
                
                if setor == s:
                    department[setor].append(newPerson)
        
            return_array.append(department)
        
        
        
        return return_array
    
    
    def get_machines(self):
        stmt = """SELECT Maquinas.id_maquina as id_maquina
, rtrim(Maquinas.ds_maquina) AS ds_maquina
, CelulaProducao.id_celulaproducao
, rtrim(CelulaProducao.ds_celulaproducao) AS CCusto
, rtrim(CelulaProducao.Observacao) AS Observacao
 FROM CelulaProducao AS CelulaProducao  WITH(NOLOCK) 
INNER JOIN Entidade AS Entidade  WITH(NOLOCK) ON (Entidade.Id_Ent = CelulaProducao.id_Empresa)
INNER JOIN PlanoCusteio AS PlanoCusteio  WITH(NOLOCK) ON (PlanoCusteio.id_CCusto = CelulaProducao.id_CCusto)
LEFT JOIN MaquinasdaCelula AS MaquinasdaCelula  WITH(NOLOCK) ON (MaquinasdaCelula.id_celulaproducao = CelulaProducao.id_celulaproducao AND MaquinasdaCelula.PierSitReg = 'ATV')
INNER JOIN Maquinas AS Maquinas  WITH(NOLOCK) ON (Maquinas.id_maquina = MaquinasdaCelula.id_maquina)
WHERE (Entidade.cdEnt = 1)
 AND (CelulaProducao.PierSitReg = 'ATV')

ORDER BY CelulaProducao.cd_celulaproducao
        """
        
        result = pd.read_sql_query(stmt, self.engine)
        
        setores = result[['CCusto', 'id_celulaproducao']].drop_duplicates()
        
        print(setores)
        
        return_array = []
        
        for index, row in setores.iterrows():
            id = row['id_celulaproducao']
            setor = row['CCusto']
            department = {}
            
            department[f'{id}-{setor}'] = []
            
            for index, row in result.iterrows():
                
                s = row['CCusto']
                ds_maquina = row['ds_maquina']
                id_maquina = row['id_maquina']
                observacao = row['Observacao']
                
                newMachine = {
                    'ds_maquina': ds_maquina,
                    'id_maquina': id_maquina,
                    'observacao': observacao
                }
                
                if setor == s:
                    department[f'{id}-{setor}'].append(newMachine)
        
            return_array.append(department)
        return return_array
    
    def insert_new_record(self, data):
        id_of = data['id_of'] 
        id_LstProcOF = data['id_LstProcOF']
        id_operacao = data['id_operacao']
        id_maquina = data['id_maquina']
        id_Prof_Intern = data['id_Prof_Intern']
        dateTime = data['dateTime'] 
        hour = data['hour']
        
        stmt = """exec ProcInsUpdApnProdTempoOnLine 0, :id_of, :id_LstProcOF, 1, :id_operacao, :id_maquina, :id_Prof_Intern, 0, :dateTime, :hour, NULL,'1900-01-02 00:00:00', 0, 0, 'N','ATV'"""
        
        params = dict(id_of = id_of, id_LstProcOF = id_LstProcOF, id_operacao = id_operacao, id_maquina = id_maquina, id_Prof_Intern= id_Prof_Intern, dateTime = dateTime, hour = hour)
        
        with self.engine.connect() as conn:
            result = conn.execute(text(stmt), params).scalar()
            conn.commit()
            return result
        
    def finalize_record(self, data):
        DataFim = data['DataFim']
        HoraFim = data['HoraFim']
        QtdProduzida = data['QtdProduzida']
        QtdRefugo = data['QtdRefugo']
        id_apontProd = data['id_apontProd']
        
        
        stmt = """ update ApnProdTempoOnline
                set DataFim = :DataFim, HoraFim = :HoraFim, QtdProduzida = :QtdProduzida, QtdRefugo = :QtdRefugo
                where id_apontProd = :id_apontProd
                """       
                
        params = dict(DataFim = DataFim, HoraFim = HoraFim, QtdProduzida = QtdProduzida, QtdRefugo = QtdRefugo, id_apontProd = id_apontProd)
        
        with self.engine.connect() as conn:
            result = conn.execute(text(stmt), params)
            conn.commit()
            return id_apontProd
        
    def insert_new_process(self, data):
        id_of = data['id_of'] 
        id_CtrlProjPCP = data['id_CtrlProjPCP']
        id_operacao = data['id_operacao']
        id_maquina = data['id_maquina']
        id_celulaproducao = data['id_celulaproducao']
        Seq_operacao = data['Seq_operacao'] 
        
        all_process = """
            select * from LstProcOF
            where id_of = ? and PierSitReg = 'ATV'"""
        
        params_all_process = (id_of, )
        
        processos = pd.read_sql_query(all_process, self.engine, params=params_all_process)
        
        qtd_processos = int(len(processos.index))
        
        id_last_process = int(processos[processos['UltProc'] == 'S']['Id_LstProcOF'].values[0])
        
        
        stmt = """exec ProcInsUpdLstProcOF 0, 
            :id_of, 
            :id_CtrlProjPCP, 
            :Seq_operacao, 
            :id_celulaproducao, 
            :id_maquina, 
            '',
            1, 
            '', 
            :id_operacao, 
            '', 
            1,
            1, 
            1,
            1, 
            1,
            'NOR',
            'N',
            0,
            'NOR',
            0,
            0,
            0,
            'ATV'
            """
        
        params = dict(id_of = id_of, 
            id_CtrlProjPCP = id_CtrlProjPCP, 
            Seq_operacao = qtd_processos, 
            id_celulaproducao = id_celulaproducao, 
            id_maquina = id_maquina, 
            id_operacao = id_operacao 
            )
        
        update_last_process_stmt = """update LstProcOF
                                set Seq_operacao = :Seq_operacao
                                where Id_LstProcOF = :id_last_process """
        update_last_process_params = dict(Seq_operacao = qtd_processos + 1, id_last_process = id_last_process)
        
        with self.engine.connect() as conn:
            conn.execute(text(update_last_process_stmt), update_last_process_params)
            result = conn.execute(text(stmt), params).scalar()
            conn.commit()
            return result
        
    def get_raw_material(self, id_of):
        stmt = """select A.id_lstPartesOF, A.id_of, A.id_MatPrima, b.cd_Referencia, rtrim(B.ds_Prod) as ds_Prod, A.qtdItem, C.cd_unidMed  
                from LstPartesOF						A
                inner join materiais					B on A.id_MatPrima = B.id_produto
                inner join unid_medidas					C on C.id_unidMed = A.id_unidMed
                where id_of = ?
                """
        params = (id_of, )
        result = pd.read_sql_query(stmt, self.engine, params=params)
        
        return result.to_dict(orient='records')
    
    def get_auxiliar_orders(self, id_of):
        stmt = """select A.id_of as id_of_pai, 
                D.id_of as id_of_filho, 
                D.cd_of as cd_of_filho,
                E.cd_Referencia,
                E.ds_prod,
                D.Quantidade,
                D.QtdProduzida,
                D.StatusOF
                from ProgramacaoProducao						A
                inner join RlcProgramacao						B on A.id_ProgProdPCP = B.id_ProgProdPCP
                inner join programacaoProducao					C on C.id_ProgProdPCP = B.IDProgProdPCPAnt
                inner join ordemFabricacao						D on D.id_of = C.id_of
                inner join materiais							E on E.id_produto = D.id_Produto
                where A.id_of = ? and A.pierSitReg = 'ATV' and B.pierSitReg = 'ATV' and C.pierSitReg = 'ATV' and D.pierSitReg = 'ATV'
                """
        params = (id_of, )
        
        result = pd.read_sql_query(stmt, self.engine, params=params)
        
        return result.to_dict(orient='records')
    
    def get_operations(self):
        stmt = """select id_operacao, 
        cd_operacao, 
        RTRIM(ds_operacao) as ds_operacao, 
        RTRIM(Observacao) as Observacao 
        from OperacoesProdutivas
        where PierSitReg = 'ATV'
        """
        
        result = pd.read_sql_query(stmt, self.engine)
        
        return result.to_dict(orient='records')
        
    def create_engine(self, uid, pwd):
        connection_string = f"DRIVER={{{os.getenv('DRIVER')}}};SERVER={os.getenv('SERVER')};DATABASE={os.getenv('DATABASE')};UID={uid};PWD={pwd}"
        url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_string})
        self.engine = create_engine(url)
        print(os.getenv('SERVER'), os.getenv('DATABASE'))
        
        
areco = Areco()
        
