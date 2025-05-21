import psycopg2
schemas = ["schema1", "schema2", "schema3"]  # Adicione os schemas desejados aqui

# Conexão ao banco de dados de Homologação de exemplo (PostgreSQL)
connHml = psycopg2.connect(
    database="exemplo",
    host="1.1.1.1",
    user="exemplo",
    password="******",
    port="5432"
)

# Conexão ao banco de dados de Produção de exemplo (PostgreSQL)
connProd = psycopg2.connect(
    database="exemploProd",
    host="1.1.1.1",
    user="exemplo",
    password="*****",
    port="5432")

# Criando um cursor para o banco de dados de homologação e produção
cursorHml = connHml.cursor()
cursorProd = connProd.cursor()


for schema in schemas:
    # Executando a consulta para obter o nome das tabelas no schema do loop
    cursorHml.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE';
    """, (schema,))

    # Recuperando os resultados da consulta
    tabelas = cursorHml.fetchall()

    for tabela in tabelas:
        # Executando a consulta para obter os nomes das colunas para cada tabela
        cursorHml.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s 
            AND table_name = %s;
        """, (schema, tabela[0]))
        
        # Recuperando os nomes das colunas
        colunas = cursorHml.fetchall()
        colunasTabela = [col[0] for col in colunas]

        # Construindo a consulta SELECT
        query = f"SELECT {', '.join(colunasTabela)} FROM {schema}.{tabela[0]}"
        
        # Executando a consulta SELECT
        cursorHml.execute(query)
        result = cursorHml.fetchall()


        # Construindo e executando as consultas INSERT para cada linha do resultado
        for res in result:
            values_placeholder = ", ".join(["%s"] * len(res))
            queryInsert = f"INSERT INTO {schema}.{tabela[0]} (" + ", ".join(colunasTabela) + ") VALUES (" + values_placeholder + ");"
            try:
                cursorProd.execute(queryInsert, res)  # Passando os valores como parâmetros
            except psycopg2.Error as ex:
                # Verifica se a exceção é relacionada a uma violação de chave estrangeira
                if "violates foreign key constraint" in str(ex):
                    continue
                print(ex)
            except Exception as ex:
                print(ex)
        # Commit nas transações do banco de dados de produção
        connProd.commit()

# Fechando o cursor e a conexão do banco de dados de homologação
cursorHml.close()
connHml.close()

# Fechando o cursor e a conexão do banco de dados de produção
cursorProd.close()
connProd.close()
