# saspy docs:
# https://sassoftware.github.io/saspy/api.html

import saspy
import pandas as pd
from IPython.display import clear_output

class AmigoSAS:
    def __init__(self,
                 cfgname: str,
                 libs: dict[str, str],
                 is_notebook: bool=True):
        """
        Classe destinada a consultas rápidas e extrações de tabelas no SAS.

        Args:
                cfgname(str): definição de configuração, o valor deve estar no sascfg_personal.py.
                libs(dict[str, str]): Dicionário contendo o nome da lib SAS como chave e seu caminho como valor.
                is_notebook(bool): Define se a classe está sendo rodada em um Jupyter Notebook. Destinada apenas à maior comodidade para análises. Seu valor default é True.
        """

    self.cfgname = cfgname
    self.libs = libs
    self.is_notebook = is_notebook
    self.session = self.init_sas()

    def init_sas(self) -> saspy.SASsession:
        """
        Inicializa a sessão no SAS.

        Args:
            None

        Returns:
            saspySASsession: Objeto saspy.
        """
        sas = saspy.SASsession(
                cfgname = self.cfgname,
                libs = self.libs
                )

        for key, value in self.libs.items():
            sas.saslib(
                    libref=key,
                    path=value
                    )
        return sas

    def reconecta(func):
        def wrapper(self, *args, **kwargs):
            """
            Decorator que realiza uma reconexão da sessão caso esta se desconecte.
            """
            try:
                resultado = func(self, *args, **kwargs)
            except:
                self.session = self.init_sas()
                if self.is_notebook:
                    clear_output()
                resultado = func(self, *args, **kwargs)
            return resultado
        return wrapper

    @reconecta
    def lista_libs(self) -> list[str]:
        """
        Retorna as libs disponíveis para o usuário para consulta.

        Args:
            None

        Returns:
            list[str]: Lista contendo as libs disponíveis para consulta.
        """
        return self.session.assigned_libref() 

    @reconecta
    def lista_tables(self, lib: str) -> list[tuple]:
        """
        Retorna as tabelas dispníveis na lib especificada.

        Args:
            lib(str): Nome da biblioteca a ser consultada.

        Returns:
            list[tuple]: Lista de tuplas contendo as tabelas disponíveis ao usuário dentro da lib.
        """
        return self.session.list_tables(libref=lib) 

    @reconecta
    def tabela_exemplo(self, lib: str, table: str) -> saspy.sasdata.SASdata:
        """
        Retorna os cinco primeiro elementos da tabela e lib especificadas. Útil para datasets grandes, onde se quer apenas analisar o teor de seu conteúdo.

        Args:
           lib(str): Nome da biblioteca a ser consultada.
           table(str): NOme da tabela a ser consultada.

       Returns:
           df(saspy.sasdata.SASdata): Primeiros cinco elementos da tabela especificada.

        """
        return self.session.sasdata(libref=lib, table=table)

    @reconecta
    def query_sas(self,
                  lib: str,
                  table: str,
                  query: str = '',
                  method: str = 'MEMORY') -> pd.DataFrame:
        """
        Realiza uma query na tabela especificada.

        Args:
           query(str): FIltro 'where' a ser feito no SAS. O valor padrão é uma string vazia, logo, retorna toda a tabela.
           lib(str): Biblioteca a ser feita a busca.
           table(str): Tabela a ser feita a busca.

       Returns:
           df(pd.DataFrame): DataFrame contendo os dados referentes a query feita. 
        """
        # Definir dsopts como o argumeto query
        dsopts = query

        # Se query não estiver vazio
        if query != '':
            # Definir o valor de 'where' de dsopts como o argumento query.
            dsopts = {'where': query}

        df = self.session.sasdata2dataframe(
                table = table,
                dsopts = dsopts,
                libref = lib,
                method = method
                )

        # Testa se o DataFrame foi criado, se sim, retorná-lo
        try:
            df.head()
            return df

        except AttributeError as e:
            print('Query inválida')
            return

    @reconecta
    def chmod_sas(self, lib_path: str, table: str) -> None:
        """
        Atuailza as permissões da tablea especificada, permitindo leitura do grupo.
        """
        return self.session.submitLOG(f"""
                                       x 'chmod 744 {lib_path}/{table}.sas7bdat';
                                       """)
