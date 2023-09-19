"""
Script em python que realiza o webscraping da pagina:

http://www.ufcstats.com/statistics/events/completed

E retorna os dataframe já tratados para analise.

Composto por três funções principais.

get_events() - Retorna um dataframe com todos os eventos já realizados

get_fighters() - Retorna dataframe com os lutadores

get_fights() - Retorna os dados das lutas que já aconteceram, relacionando com os eventos

"""

# Importando bibliotecas

# Webcraping  
import requests 
from bs4 import BeautifulSoup

# Manipulação de dados
import pandas as pd
import numpy as np



"""
Criando a função get_events() que retorna um data frame tratado com todos os eventos registrados no site UFCstats. A documentação desta função, bem como todas as atualizações realizadas, estão no arquivo note_get_events.ipynb.
"""

def get_events(bruto=False):
    """
    Função que retorna o data frame com todos os eventos realizados pelo UFC e armazenados no site UFCstats.
    """
    # Link de acesso para pagina
    URL = "http://www.ufcstats.com/statistics/events/completed?page=all"


    # Realizando o request para o site 
    response = requests.get(URL)
    response.raise_for_status()

    # Obtendo pagina completa em html
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Encontrando a tabela com a classe especificada
    table = soup.find("table", class_="b-statistics__table-events")

    # Transformando tabela de html para dataframe pandas ou retornando erro se não encontrar a tabela
    if table:
        # Extraindo cabeçalho
        header = [th.text.strip() for th in table.find('thead').find_all('th')]

        # Extraindo as linhas
        rows = table.find('tbody').find_all('tr')
        table_data = [[col.text.strip() for col in row.find_all('td')] for row in rows]

        # Convertendo em dataframe
        df_bruto = pd.DataFrame(table_data[1:], columns=header) #Obs: Não considera a primeira linha

    else:
        return print("Não foi possível encontrar a tabela desejada na página.")


    if bruto:
        return df_bruto

    """
    A parte do código abaixo é dedicata ao tratamento do df_bruto para os parametros pré estabelecidos no arquivo note_get_events.ipynb.
    """

    # Criando o dataframe final
    df_final = df_bruto.copy()

    # Ajustando coluna evento
    df_final['event'] = df_final['Name/date'].str.split("\n")
    df_final['event'] = df_final['event'].apply(lambda x: x[0])

    # Ajustando data
    df_final['date'] = df_final['Name/date'].apply(lambda x: x.split("\n")[-1].strip())
    df_final['date'] = pd.to_datetime(df_final['date'])

    # Retirando coluna Name/date
    df_final = df_final.drop(columns=["Name/date"])

    # Criando colunas de cidade, estado e pais
    # Cidade
    df_final['city'] = df_final['Location'].apply(lambda x: x.split(",")[0].strip())

    # Estado
    df_final['state'] = df_final['Location'].apply(lambda x: x.split(",")[1].strip() if len(x.split(",")) == 3 else np.nan)

    # Pais
    df_final['country'] = df_final['Location'].apply(lambda x: x.split(",")[-1].strip())

    # Removendo a coluna Location
    df_final = df_final.drop(columns=["Location"])


    """
    A parte do código a seguir, cria uma primary key para a tabela de evetos, para mais detalhes olhar o arquivo documentado note_get_events.ipynb
    """

    # Criando range com o index invertido
    inverted_index = df_final.index[::-1] + 1

    # Criando a chave primaria no formato EVEXXXX
    df_final['event_id'] = ["EVE" + f"{index:04}" for index in inverted_index]

    """
    Como ultimo tratamento, as colunas serão reorganizadas
    """
    df_final = df_final[["event_id", "event", "date","city", "state", "country"]]

    return df_final



"""
A parte do código a seguir contempla o webscraping e tratamentos para obter um data frame final com informações dos lutadores da pagina UFCstats. A documentação desta função esta no arquivo note_get_fighters.ipynb.
"""

# Criando função auxiliar que realiza o scrap dos lutadores de apenas uma letra

def letter_get_fighters(letra):
    """
    Função que realiza o scrap da pagina e retorna um dataframe pandas com a letra especificada

    Obs: A letra deve esta minuscula
    """

    # Link de acesso para pagina com a letra especificada
    URL = f"http://www.ufcstats.com/statistics/fighters?char={letra}&page=all"

    # Realizando request para o site
    response = requests.get(URL)
    response.raise_for_status()

    # Obtendo pagina completa em html
    soup = BeautifulSoup(response.content, "html.parser")

    # Encontrando tabela com a classe especificada
    table = soup.find("table", class_="b-statistics__table")

    # Transformando tabela de html para dataframe pandas ou retornando erro se não encontrar a tabela
    if table:
        # Extraindo cabeçalho
        header = [th.text.strip() for th in table.find('thead').find_all('th')]

        # Extraindo as linhas
        rows = table.find('tbody').find_all('tr')
        table_data = [[col.text.strip() for col in row.find_all('td')] for row in rows]

        # Convertendo em dataframe
        df_bruto = pd.DataFrame(table_data[1:], columns=header) #Obs: Não considera a primeira linha

        return df_bruto
    else:
        return print("Não foi possível encontrar a tabela desejada na página.")
    

"""
A partir da função acima, executa-se a função abaixo iterando sobre todas as letras do alfabeto para obter um dataframe completo.
"""


def get_fighters(bruto=False):
    """
    Função que retorna todos os lutadores com dados presentes no site UFCstats.
    """
    # Lista com todas as letras do alfabeto para iteração
    alfabeto = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

    # Criando df_bruto para armazenar os dados da iteração 
    df_bruto = pd.DataFrame(columns=["First", "Last", "Nickname", "Ht.", "Wt.", "Reach", "Stance", "W", "L", "D", "Belt"])

    for letra in alfabeto:
        # Data frame provisorio que armazena os lutadores da letra da iteração
        df_provisorio = letter_get_fighters(letra)

        # Concatenando o resultado no df_bruto
        df_bruto = pd.concat([df_bruto, df_provisorio], ignore_index=True)


    # Criando instancia para retornar df_bruto
    if bruto:
        return df_bruto

    """
    Abaixo estão os tratamentos realizados até o retorno do df_final
    """

    # Criando cópia do df_bruto que será retornado ao final da função
    df_final = df_bruto.copy()

    # Primeiro tratamento será a criação da coluna height
    # Transformando todos os -- da coluna Ht. em np.nan
    df_final['Ht.'] = df_final["Ht."].apply(lambda x: np.nan if x == "--" else x)


    # Criando função que trata e retorna valor em centimetros
    def feet_cm(x):
        if x == np.nan:
            return np.nan
        else:
            feet = float(str(x).split(" ")[0].replace("'", ""))
            inches = float(str(x).split(" ")[-1].replace('"', ''))

            cm = (feet*30.48) + (inches*2.54)

            return cm

    # Criando coluna height com a transformação correta
    df_final["height"] = df_final['Ht.'].apply(feet_cm)


    # Realizando tratamento da coluna de alcance (Reach)
    # Transformando todos os -- da coluna Reach. em np.nan
    df_final['Reach'] = df_final["Reach"].apply(lambda x: np.nan if x == "--" else x)

    # Criando coluna de alcance em cm
    df_final['reach'] = df_final['Reach'].apply(lambda x: float(str(x).replace('"', ""))*2.54 if x != np.nan else np.nan)


    # Tratamento na coluna de peso
    df_final['weight'] = df_final['Wt.'].apply(lambda x: round(float(str(x).split(" ")[0])*0.453592, 3) if x != "--" else np.nan)


    # Excluindo colunas desnecessarias
    df_final = df_final.drop(columns=["Ht.", "Wt.", "Reach"])


    # Criando coluna de total fights e tratando formato
    # Ajustando formato das colunas
    df_final['win'] = df_final["W"].astype("int64")

    df_final['lose'] = df_final["L"].astype("int64")

    df_final['draw'] = df_final["D"].astype("int64")

    # Criando coluna de número total de lutas no ufc
    df_final['total_fights'] = df_final['win'] + df_final["lose"] + df_final["draw"]

    # Apagando colunas W,L e D
    df_final = df_final.drop(columns=["W", "L", "D"])



    """
    Com todas as colunas criadas, a parte do código a seguir cria primary keys para esta tabela

    OBS: Para que esta parte funcione o arquivo fighters_primary_key_reference.csv deve estar na mesma pasta
    """

    # Conferindo se temos lutadores novos
    # Criando coluna parcial
    df_final['full_name_conference'] = (df_final["First"] + df_final["Last"] + df_final["Nickname"])

    # Lendo arquivo csv de referencia
    df_reference_keys = pd.read_csv("fighters_primary_key_reference.csv")

    # Fazendo conferencia de nomes
    old_fighters_names = df_reference_keys['full_name_conference'].to_list()
    new_fighters_names = df_final['full_name_conference'].to_list()

    no_primary_key_fighters = [name for name in new_fighters_names if name not in old_fighters_names]

    if len(no_primary_key_fighters) > 0 :
        print(f"Os seguintes lutadores não possuem primarys keys: {no_primary_key_fighters}")

    # Adicionando primary_key ao dataframe
    df_final = df_final.merge(df_reference_keys[["full_name_conference", "fighters_id"]], on="full_name_conference", how="left")

    # Apagando coluna de referencia para primary keys
    df_final = df_final.drop(columns=["full_name_conference"])

    # Renomeando as colunas para manter padrão de letras minusculas 
    df_final.rename(columns={"First": "first", "Last":"last", "Nickname": "nickname", "Stance": "stance", "Belt": "belt"})

    return df_final



"""
O trecho de script a seguir descreve a função get_fights e suas respectivas funções auxiliares. Esta função, ao ser executada, produz um dataframe processado contendo todas as lutas registradas no site UFCstats.

Nota: A função itera através de todos os links encontrados na tabela de eventos. Devido a essa atividade intensiva de requisições ao site UFCstats, há um risco do site detectar e bloquear o IP do usuário se a função for executada repetidamente.
"""

# Função que retorna o nome do evento e o link.

def get_links_fights():
    """
    Função que retorna dicionario de links da tabela de lutas realizada presente na pagina UFCstats e o seu respectivo evento.
    """

    #Link de acesso para pagina 

    URL = "http://www.ufcstats.com/statistics/events/completed?page=all"

    # Realizando o request para o site
    response = requests.get(URL)
    response.raise_for_status()

    # Obtendo a pagina completa em html
    soup = BeautifulSoup(response.content, "html.parser")

    # Encontrando tags <a> com a classe especificada
    links = soup.find_all("a", class_="b-link b-link_style_black")

    # lista para armazenar os eventos
    events = []

    # Lista para armazenar os hrefs
    hrefs = []

    # Salvando os links na lista
    for tag in links:
        hrefs.append(tag["href"])
        events.append(tag.text)

    # Tratando events
    events = [event.split("\n")[1].split("                          ")[-1] for event in events]

    # Criando dicionario a partir das listas
    dict_link = dict(zip(events, hrefs))

    return dict_link


# Função que realiza o scrap bruto de um unico evento

def fight_scrap_table(event,link,bruto=False):
    """ 
    Função que realiza o scrap e tratamento de cada evento
    """
    # Link de acesso para pagina
    URL = link

    # Realizando request para o site
    response = requests.get(URL)
    response.raise_for_status()

    # Obtendo pagina complera em html
    soup = BeautifulSoup(response.content, "html.parser")

    # Encontrando tabela
    table = soup.find("table", class_="b-fight-details__table b-fight-details__table_style_margin-top b-fight-details__table_type_event-details js-fight-table")

    # Transformando tabela de html para dataframe pandas ou retornando erro se não encontrar a tabela
    if table:
        # Extraindo cabeçalho
        header = [th.text.strip() for th in table.find('thead').find_all('th')]

        # Extraindo as linhas
        rows = table.find('tbody').find_all('tr')
        table_data = [[col.text.strip() for col in row.find_all('td')] for row in rows]

        # Convertendo em dataframe
        df_bruto = pd.DataFrame(table_data, columns=header) #Obs: Não considera a primeira linha

    else:
        return print("Não foi possível encontrar a tabela desejada na página.")

    # Criando coluna com o evento
    df_bruto["event"] = event

    if bruto:
        return df_bruto
    
    return df_bruto


# Função final que itera os eventos e retorna um dataframe tratado.

def get_fights(bruto=False,id=True):
    """
        Função que retorna o dataframe com todas as lutas realizadas e seus respectivos eventos
    """

    # Executando função e obtendo dicionario com eventos e links
    dicionario_event_link = get_links_fights()

    # Criando df_bruto
    df_bruto = pd.DataFrame()

    # Criando loop para realizar a iteração
    for event, link in dicionario_event_link.items():
        # Obtendo dataframe parcial 
        df_parcial = fight_scrap_table(event, link)

        # Criando coluna de evento
        df_parcial['event'] = event

        # Merge no dataframe bruto
        df_bruto = pd.concat([df_bruto, df_parcial], axis=0)

    if bruto:
        return df_bruto
    
    """
        A parte do script abaixo é dedicada a tratar o dataframe.
    """

    # Criando função final
    df_final = df_bruto.copy()

    # Tratando coluna Fighter
    # Obtendo coluna fighter1
    df_final["fighter1"] = df_final["Fighter"].str.split("\n").apply(lambda x: x[0])

    # Obtendo coluna fighter2
    df_final["fighter2"] = df_final['Fighter'].str.split("              ").apply(lambda x: x[-1])

    # Dropando coluna Fighter
    df_final = df_final.drop(columns=["Fighter"])



    # Trando coluna W/L
    df_final['w/l'] = df_final["W/L"].apply(lambda x: "nc" if x == "nc\n\n\nnc" else "draw" if x == "draw\n\n\ndraw" else "win")

    # Dropando coluna com letra maiuscula
    df_final = df_final.drop(columns=["W/L"])


    # Fighter 1
    df_final["kd_fighter1"] = df_final['Kd'].str.split("\n").apply(lambda x: x[0])
    df_final["str_fighter1"] = df_final['Str'].str.split("\n").apply(lambda x: x[0])
    df_final["td_fighter1"] = df_final['Td'].str.split("\n").apply(lambda x: x[0])
    df_final["sub_fighter1"] = df_final['Sub'].str.split("\n").apply(lambda x: x[0])

    # Fighter 2
    df_final["kd_fighter2"] = df_final['Kd'].str.split(" ").apply(lambda x: x[-1])
    df_final["str_fighter2"] = df_final['Str'].str.split(" ").apply(lambda x: x[-1])
    df_final["td_fighter2"] = df_final['Td'].str.split(" ").apply(lambda x: x[-1])
    df_final["sub_fighter2"] = df_final['Sub'].str.split(" ").apply(lambda x: x[-1])

    # Dropando colunas após o tratamento
    df_final = df_final.drop(columns=["Kd", "Str", "Td", "Sub"])



    # Tratando a coluna Method
    df_final["method"] = df_final["Method"].str.replace("\n","").str.replace("             ","").str.replace(" ","_")

    # Dropando coluna Method
    df_final = df_final.drop(columns=["Method"])

    # Renomeando as colunas com letras maiusculas
    df_final = df_final.rename(columns={"Weight class": "weight_class", "Round":"round", "Time":"time"})   

    # Reordenando o dataframe
    df_final = df_final[["event", "weight_class", "w/l","fighter1", "fighter2",
                                  "kd_fighter1", "kd_fighter2", "str_fighter1", "str_fighter2",
                                  "td_fighter1", "td_fighter2", "sub_fighter1", "sub_fighter2",
                                  "method", "round", "time"]]
    

    """
        Parte do scrip que retorna as primary keys de cada evento ao invés do nome.
    """

    if id:
        # Obtendo as chaves de cada evento
        df_event_key = get_events()[["event", "event_id"]]

        # Realizando o merge com o df_final
        df_final = df_final.merge(df_event_key, on="event", how="left")

        # Dropando coluna event
        df_final = df_final.drop(columns=["event"])

        # Reorganizando ordem das colunas
        cols = ["event_id"] + [col for col in df_final.columns.to_list() if col != "event_id"]
        df_final = df_final[cols]

        return df_final
    else:
        return df_final

