import streamlit as st
import psycopg2
from psycopg2 import Error
import hashlib
from streamlit_option_menu import option_menu
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import io
import os
import re
import toml

# Carregar variáveis de ambiente

username = st.secrets["database"]['user']
password = st.secrets["database"]['password']
host = st.secrets["database"]['host']
database = st.secrets["database"]['name']
port = st.secrets["database"]['port']

# Verifique se todas as variáveis de ambiente estão carregadas
if not all([username, password, host, port, database]):
    st.error("Erro: Variáveis de ambiente do banco de dados não estão definidas corretamente.")
    st.stop()

# Configurações de conexão com o banco de dados para autenticação
db_config = {
    'host': host,
    'port': port,
    'user': username,
    'password': password,
    'database': database
}

# Função de conexão com o PostgreSQL usando psycopg2
def create_db_connection():
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Error as e:
        st.error(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
        return None

# Função de conexão usando SQLAlchemy (para pandas)
@st.cache_resource
def get_engine():
    try:
        engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")
        # Teste a conexão
        engine.connect()
        return engine
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        st.stop()

engine = get_engine()

# Função para autenticar usuários
def login_user(username, password):
    conn = create_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            query = "SELECT id FROM semrush_qa.users WHERE username = %s AND password = %s"
            cursor.execute(query, (username, hashed_password))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            if result:
                st.session_state['logged_in'] = True
                st.session_state['user_id'] = result[0]  # ID do usuário
                st.session_state['username'] = username  # Salva o nome de usuário na sessão
                return True
            else:
                return False
        except Error as e:
            st.error(f"Erro ao verificar as credenciais: {e}")
            return False

# Função para criar um novo usuário
def create_user(username, email, password, secret_question, secret_answer):
    conn = create_db_connection()
    if conn:
        try:
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            hashed_secret_answer = hashlib.sha256(secret_answer.encode()).hexdigest()
            cursor = conn.cursor()
            query = """
            INSERT INTO semrush_qa.users (username, email, password, secret_question, secret_answer) 
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (username, email, hashed_password, secret_question, hashed_secret_answer))
            conn.commit()
            cursor.close()
            conn.close()
            st.success("Usuário cadastrado com sucesso!")
        except Error as e:
            st.error(f"Erro ao inserir o usuário no banco de dados: {e}")

# Função para recuperação de senha
def recover_password(username, secret_question, secret_answer, new_password):
    conn = create_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = """
            SELECT id FROM semrush_qa.users WHERE username = %s AND secret_question = %s AND secret_answer = %s
            """
            cursor.execute(query, (username, secret_question, secret_answer))
            result = cursor.fetchone()

            if result:
                hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
                update_query = "UPDATE semrush_qa.users SET password = %s WHERE id = %s"
                cursor.execute(update_query, (hashed_password, result[0]))
                conn.commit()
                st.success("Senha atualizada com sucesso!")
            else:
                st.error("As informações fornecidas estão incorretas.")

            cursor.close()
            conn.close()
        except Error as e:
            st.error(f"Erro ao recuperar a senha: {e}")

# Função para buscar dados (pandas + SQLAlchemy)
@st.cache_data
def get_data(display_date=None, targets_filter=None, selected_columns=None):
    try:
        query = "SELECT * FROM semrush_qa.traffic_analytics"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)

        # Aplicar filtro de data se selecionado
        if display_date:
            df['display_date'] = pd.to_datetime(df['display_date'], errors='coerce').dt.date
            df = df[df['display_date'] == display_date]

        # Aplicar filtro de targets se selecionado
        if targets_filter:
            df = df[df['targets'].str.contains(targets_filter, case=False, na=False)]

        # Filtrar colunas selecionadas
        if selected_columns:
            df = df[selected_columns]

        return df
    except Exception as e:
        st.error(f"Erro ao buscar dados do banco: {e}")
        return pd.DataFrame()  # Retorna DataFrame vazio em caso de erro

# Função principal para exibir a interface de dados
def visualizacao_de_dados():
    st.title("Visualização de Dados - Banco de Dados")

    # Container para os filtros
    with st.container():
        col1, col2 = st.columns([1, 3])

        # Filtro de data usando date_input
        with col1:
            display_date = st.date_input(
                "Selecione a data:",
                value=None,
                help="Selecione a data no formato YYYY-MM-DD"
            )

        with col2:
            # Filtro de texto para buscar targets/domínios
            targets_filter = st.text_input(
                "Filtro de Targets (Domínios):",
                value="",
                help="Digite o domínio ou parte dele para buscar (ex: 'uol')"
            )

        # Busca inicial para obter todas as colunas
        try:
            initial_query = "SELECT * FROM semrush_qa.traffic_analytics"
            initial_df = pd.read_sql(initial_query, engine)
            available_columns = initial_df.columns.tolist()
        except Exception as e:
            st.error(f"Erro ao buscar colunas do banco de dados: {e}")
            available_columns = []

        # Definir as colunas padrão
        default_columns = ['targets', 'display_date', 'rank', 'users', 'bounce_rate']
        default_columns = [col for col in default_columns if col in available_columns]

        # Filtro de colunas com multiselect
        columns = st.multiselect(
            "Selecione as colunas que deseja visualizar:",
            options=available_columns,
            default=default_columns
        )

        # Botão para aplicar filtros
        apply_filters = st.button("Aplicar Filtros")

    # Buscar os dados com base nos filtros aplicados
    if apply_filters:
        with st.spinner("Buscando dados..."):
            data = get_data(display_date=display_date, targets_filter=targets_filter, selected_columns=columns)

        if not data.empty:
            st.success(f"Dados filtrados com sucesso!")
            st.dataframe(data)

            # Opções de download
            try:
                csv = data.to_csv(index=False).encode('utf-8')
                excel = io.BytesIO()
                with pd.ExcelWriter(excel, engine='xlsxwriter') as writer:
                    data.to_excel(writer, index=False, sheet_name='Dados')
                excel.seek(0)

                # Botões de download
                col1, col2 = st.columns(2)
                col1.download_button(
                    label="Baixar dados filtrados em CSV",
                    data=csv,
                    file_name=f'dados_filtrados.csv',
                    mime='text/csv'
                )
                col2.download_button(
                    label="Baixar dados filtrados em XLSX",
                    data=excel,
                    file_name=f'dados_filtrados.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            except Exception as e:
                st.error(f"Erro ao gerar arquivos de download: {e}")
        else:
            st.warning("Nenhum dado encontrado com os filtros aplicados.")
    else:
        st.info("Aplique os filtros para visualizar os dados.")

# Função para extrair domínio das URLs
def extrair_dominio(url):
    if not isinstance(url, str):
        return None
    # Verificar se a URL é do G1
    if 'g1.globo.com' in url:
        # Regex para verificar se é uma página específica do estado
        match_estado = re.search(r'g1\.globo\.com/([a-z]{2})/', url)
        if match_estado:
            estado = match_estado.group(1)
            return f"g1.globo.com/{estado}"  # Retorna o estado específico
        else:
            return 'g1.globo.com'  # Caso contrário, retorna o domínio geral
    else:
        # Regex para extrair o domínio principal e suas terminações
        match = re.search(r'https?://(?:www\d*\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z]{2,})+)', url)
        if match:
            return match.group(1)  # Retorna o domínio principal
        return None

# Função para buscar informações do banco de dados com base no domínio e na data
@st.cache_data
def buscar_info_dominio(dominios, db_columns, display_date):
    try:
        # Escapar aspas simples para evitar SQL injection
        dominios_escapados = [dom.replace("'", "''") for dom in dominios]
        # Criação de placeholders para parâmetros
        placeholders = ', '.join([f"'{d}'" for d in dominios_escapados])
        columns_str = ', '.join(db_columns)
        # Formatar a data para o formato SQL (assumindo YYYY-MM-DD)
        display_date_str = display_date.strftime('%Y-%m-%d')
        query = f"""
            SELECT dominio, {columns_str}
            FROM semrush_qa.traffic_analytics
            WHERE dominio IN ({placeholders})
            AND display_date = '{display_date_str}'
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar informações do domínio: {e}")
        return pd.DataFrame()
    
# Função que busca informações no banco de dados com filtro por display_date
def buscar_info_dominio(dominios, colunas, display_date=None):
    query = "SELECT * FROM semrush_qa.traffic_analytics WHERE targets IN ({})".format(
        ', '.join(["'{}'".format(d) for d in dominios])
    )
    
    if display_date:
        query += " AND display_date = '{}'".format(display_date)

    return pd.read_sql(query, engine)

def upload():
    st.title("Upload e Mapeamento de URLs")

    st.write("""
        **Passos:**
        1. Faça o upload do seu arquivo (CSV ou XLSX).
        2. Visualize os dados carregados.
        3. Selecione a coluna que contém as URLs.
        4. Extraia os domínios das URLs.
        5. Selecione a data (`display_date`) para filtrar os dados.
        6. Mapeie as colunas do banco de dados com as colunas do seu arquivo.
        7. Baixe o arquivo com as informações preenchidas.
    """)

    # Upload do arquivo
    uploaded_file = st.file_uploader("Faça o upload do seu arquivo (CSV ou XLSX)", type=['csv', 'xlsx'])

    if uploaded_file is not None:
        try:
            # Ler o arquivo com base na extensão e especificar a engine para Excel
            if uploaded_file.name.endswith('.csv'):
                df_uploaded = pd.read_csv(uploaded_file)
            else:
                df_uploaded = pd.read_excel(uploaded_file, engine='openpyxl')  # Especificar a engine

            st.success("Arquivo carregado com sucesso!")
            st.dataframe(df_uploaded.head())

            # Identificar colunas que contêm 'url' no nome
            url_columns = [col for col in df_uploaded.columns if 'url' in col.lower()]
            if not url_columns:
                st.error("Nenhuma coluna com 'url' encontrada. Por favor, verifique o arquivo.")
            else:
                selected_url_col = st.selectbox("Selecione a coluna que contém as URLs:", url_columns)

                # Extrair domínios das URLs
                df_uploaded['targets'] = df_uploaded[selected_url_col].apply(extrair_dominio)
                st.write("Domínios extraídos:")
                st.dataframe(df_uploaded[['targets']].head())

                # Buscar domínios únicos para consulta no banco
                dominios_unicos = df_uploaded['targets'].dropna().unique().tolist()
                st.write(f"Total de domínios únicos para consulta: {len(dominios_unicos)}")

                # Selecionar a data para filtrar os dados do banco
                display_date = st.date_input(
                    "Selecione a data para filtrar os dados:",
                    value=datetime.today().date(),  # Data padrão como hoje
                    help="Selecione a data no formato YYYY-MM-DD"
                )

                # Seleção das colunas do banco para mapear
                try:
                    initial_query = "SELECT * FROM semrush_qa.traffic_analytics"
                    with engine.connect() as connection:
                        initial_df = pd.read_sql(initial_query, connection)
                    available_db_columns = initial_df.columns.tolist()
                    if 'targets' in available_db_columns:
                        available_db_columns.remove('targets')  # Remover a coluna 'targets' para evitar duplicação
                except Exception as e:
                    st.error(f"Erro ao buscar colunas do banco de dados: {e}")
                    available_db_columns = []

                db_columns_selected = st.multiselect(
                    "Selecione as colunas do banco de dados que deseja preencher no seu arquivo:",
                    options=available_db_columns,
                    default=['users', 'bounce_rate']  # Defina um padrão conforme necessário
                )

                if db_columns_selected:
                    with st.spinner("Buscando informações no banco de dados..."):
                        # Buscar somente as colunas selecionadas
                        df_info = buscar_info_dominio(dominios_unicos, db_columns_selected, display_date)

                    if not df_info.empty:
                        st.success("Informações do banco de dados obtidas com sucesso!")
                        st.dataframe(df_info[db_columns_selected].head())

                        # Merge dos dados do arquivo com as informações do banco
                        df_merged = df_uploaded.merge(df_info[['targets'] + db_columns_selected], on='targets', how='left')

                        st.write("Arquivo com informações preenchidas:")
                        st.dataframe(df_merged.head())

                        # Mapeamento das colunas do banco para as colunas do arquivo
                        st.subheader("Mapeamento de Colunas")

                        # Criar um dicionário para mapear as colunas
                        mapping = {}
                        for db_col in db_columns_selected:
                            # Opção para selecionar a coluna do arquivo para preencher
                            file_col = st.selectbox(
                                f"Selecione a coluna no arquivo para preencher com '{db_col}':", 
                                options=[col for col in df_merged.columns] + [f"Nova coluna para {db_col}"],
                                key=db_col
                            )
                            if file_col.startswith("Nova coluna"):
                                # Solicitar o nome da nova coluna
                                new_col_name = st.text_input(f"Digite o nome da nova coluna para '{db_col}':", key=f"new_{db_col}")
                                if new_col_name:
                                    mapping[db_col] = new_col_name
                            else:
                                mapping[db_col] = file_col

                        # Aplicar o mapeamento
                        for db_col, file_col in mapping.items():
                            if file_col.startswith("Nova coluna") or file_col not in df_merged.columns:
                                # Criar a nova coluna com os dados do banco
                                if 'Nova coluna para' in file_col and 'new_' in file_col:
                                    # Evitar sobrescrever
                                    col_name = mapping[db_col]
                                    df_merged[col_name] = df_merged[db_col]
                            else:
                                # Preencher a coluna existente com os dados do banco
                                df_merged[file_col] = df_merged[db_col]

                        st.write("Arquivo final após mapeamento:")
                        st.dataframe(df_merged)

                        # Opções de download
                        st.subheader("Download do Arquivo Preenchido")

                        # Selecionar formato de download
                        download_format = st.radio(
                            "Selecione o formato para download:",
                            options=['CSV', 'XLSX']
                        )

                        if download_format == 'CSV':
                            try:
                                csv = df_merged.to_csv(index=False).encode('utf-8')
                                st.download_button(
                                    label="Baixar arquivo preenchido em CSV",
                                    data=csv,
                                    file_name='arquivo_preenchido.csv',
                                    mime='text/csv'
                                )
                            except Exception as e:
                                st.error(f"Erro ao gerar arquivo CSV: {e}")
                        else:
                            try:
                                excel = io.BytesIO()
                                with pd.ExcelWriter(excel, engine='xlsxwriter') as writer:
                                    df_merged.to_excel(writer, index=False, sheet_name='Dados')
                                excel.seek(0)
                                st.download_button(
                                    label="Baixar arquivo preenchido em XLSX",
                                    data=excel,
                                    file_name='arquivo_preenchido.xlsx',
                                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                                )
                            except Exception as e:
                                st.error(f"Erro ao gerar arquivo XLSX: {e}")
                    else:
                        st.warning("Nenhuma informação encontrada para os domínios fornecidos na data selecionada.")
                else:
                    st.warning("Por favor, selecione pelo menos uma coluna do banco de dados para preencher.")
        except Exception as e:
            st.error(f"Ocorreu um erro ao processar o arquivo: {e}")

# Função principal para o aplicativo
def main():
    # Função de logout
    def logout():
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''

    # Se estiver logado, mostrar o botão de logout e mensagem de boas-vindas
    if st.session_state.get('logged_in', False):
        st.button("Logout", on_click=logout)
        st.write(f"Bem-vindo {st.session_state['username']}!")

        # Interface principal de navegação
        with st.sidebar:
            selected = option_menu(
                menu_title=None,
                options=["Visualização de Dados", "Upload e Mapeamento de URLs"],
                icons=["bar-chart-line", "upload"],
                menu_icon="cast"
            )

        if selected == "Visualização de Dados":
            visualizacao_de_dados()
        elif selected == "Upload e Mapeamento de URLs":
            upload()

    else:
        # Interface de login e registro
        menu = ["Login", "Cadastro", "Recuperar Senha"]
        choice = st.sidebar.selectbox("Menu", menu)

        if choice == "Login":
            username = st.sidebar.text_input("Usuário")
            password = st.sidebar.text_input("Senha", type='password')

            if st.sidebar.button("Login"):
                if login_user(username, password):
                    st.success("Seja Bem-Vindo")
                    st.rerun()  # Atualiza a interface
                else:
                    st.error("Usuário ou senha incorretos.")

        elif choice == "Cadastro":
            with st.form(key='user_form'):
                username = st.text_input("Usuário")
                email = st.text_input("Email")
                password = st.text_input("Senha", type='password')
                secret_question = st.text_input("Pergunta Secreta")
                secret_answer = st.text_input("Resposta Secreta")
                submit_button = st.form_submit_button(label='Cadastrar')

                if submit_button:
                    create_user(username, email, password, secret_question, secret_answer)

        elif choice == "Recuperar Senha":
            with st.form(key='recover_form'):
                username = st.text_input("Usuário")
                secret_question = st.text_input("Pergunta Secreta")
                secret_answer = st.text_input("Resposta Secreta")
                new_password = st.text_input("Nova Senha", type='password')
                submit_button = st.form_submit_button(label='Recuperar Senha')

                if submit_button:
                    recover_password(username, secret_question, secret_answer, new_password)

if __name__ == "__main__":
    main()
