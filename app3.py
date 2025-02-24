import streamlit as st
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import urllib.parse
import pandas as pd
import time
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from newspaper import Article  # Importa o newspaper3k

# Função para seguir o link redirecionado e capturar o link final
def obter_link_original(link_google_news, navegador):
    navegador.get(link_google_news)
    time.sleep(3)  # Tempo para carregar a página redirecionada
    return navegador.current_url  # Captura o URL final após redirecionamento

# Função para extrair o conteúdo da matéria usando newspaper3k
def extrair_conteudo(url):
    try:
        artigo = Article(url, language='pt')
        artigo.download()
        artigo.parse()
        return artigo.text
    except Exception as e:
        return f"Erro ao extrair conteúdo: {e}"

# Função para buscar notícias no Google Notícias
def buscar_noticias(term, dias):
    query_encoded = urllib.parse.quote_plus(term)
    query_dias = f"{dias}d"
    query2 = urllib.parse.quote_plus(query_dias)
    url = f'https://news.google.com/search?q={query_encoded}%20when%3A{query2}&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'

    # Definir opções do Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Rodar sem interface gráfica
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    navegador = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    navegador.get(url)
    time.sleep(5)  # Tempo para carregar a página

    soup = BeautifulSoup(navegador.page_source, 'html.parser')
    noticias = soup.find_all('article')

    titulos = []
    links = []
    fontes = []
    datas = []
    links_originais = []
    conteudos = []  # Lista para armazenar o conteúdo extraído de cada matéria

    total_noticias = len(noticias)
    progresso = st.progress(0)
    tempo_inicial = time.time()

    for i, noticia in enumerate(noticias):
        elemento_a = noticia.find('a', class_='JtKRv')
        if elemento_a is not None:
            titulo = elemento_a.text
            link = elemento_a['href']
            link = link.replace('.', 'https://news.google.com', 1)
            link_original = obter_link_original(link, navegador)  # Captura o link original após redirecionamento
        else:
            titulo = 'Título não encontrado'
            link = 'Link não encontrado'
            link_original = 'Link original não encontrado'

        elemento_fonte = noticia.find('div', class_='vr1PYe')
        fonte = elemento_fonte.text if elemento_fonte else 'Fonte não encontrada'
        data = noticia.find('time')['datetime'] if noticia.find('time') else 'Data não encontrada'

        # Extrai o conteúdo da matéria utilizando o newspaper3k
        conteudo = extrair_conteudo(link_original)

        titulos.append(titulo)
        links.append(link)
        fontes.append(fonte)
        datas.append(data)
        links_originais.append(link_original)
        conteudos.append(conteudo)

        progresso_atual = (i + 1) / total_noticias
        progresso.progress(progresso_atual)

    tempo_final = time.time()
    tempo_gasto = tempo_final - tempo_inicial

    navegador.quit()

    # Cria o DataFrame com a nova coluna de conteúdo
    df = pd.DataFrame({
        'title': titulos,
        'vehicle': fontes,
        'publication_date': datas,
        'google_news_link': links,
        'url': links_originais,
        'content': conteudos
    })

    # Converte a coluna 'publication_date' para datetime
    df['publication_date'] = pd.to_datetime(df['publication_date'], errors='coerce')

    return df, tempo_gasto

import streamlit as st
import pandas as pd

def extration_news():
    st.title("Extração de Google Notícias")

    termo = st.text_input("Digite o termo para buscar as notícias")
    dias = st.slider("Selecione o intervalo de dias", 1, 365)

    if st.button("Buscar"):
        if termo:
            with st.spinner('Buscando notícias...'):
                # Chame sua função de busca; por exemplo:
                df_resultado, tempo_gasto = buscar_noticias(termo, dias)
                st.success(f'Busca concluída! Tempo gasto: {tempo_gasto:.2f} segundos')
                st.dataframe(df_resultado)

                csv = df_resultado.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Baixar resultados como CSV",
                    data=csv,
                    file_name='noticias_google.csv',
                    mime='text/csv'
                )
        else:
            st.warning("Por favor, insira um termo para busca.")

def extract_links_csv():
    st.title("Extração de Links a partir de CSV")
    # Upload do arquivo CSV
    uploaded_file = st.file_uploader("Faça o upload do arquivo CSV com os links das notícias", type=["csv"])

    if uploaded_file is not None:
        # Lendo o CSV
        df_links = pd.read_csv(uploaded_file)
        st.write("Visualização dos dados:")
        st.dataframe(df_links.head())

        # Seleciona a coluna que contém os links
        coluna_links = st.selectbox("Selecione a coluna que contém os links", df_links.columns)

        if st.button("Extrair Notícias"):
            resultados = []
            # Itera sobre os links utilizando .items()
            for index, link in df_links[coluna_links].items():
                info = {"link": link}
                try:
                    artigo = Article(link, language='pt')
                    artigo.download()
                    artigo.parse()
                    info["título"] = artigo.title
                    info["texto"] = artigo.text
                    info["status"] = "Extraído com sucesso"
                except Exception as e:
                    info["título"] = None
                    info["texto"] = None
                    info["status"] = f"Erro: {str(e)}"
                resultados.append(info)

            # Converte os resultados em DataFrame
            df_resultado = pd.DataFrame(resultados)
            st.write("Resultado da extração:")
            st.dataframe(df_resultado)

            # Permite download do resultado em CSV
            csv = df_resultado.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Baixar resultados como CSV",
                data=csv,
                file_name='noticias_extraidas.csv',
                mime='text/csv'
            )