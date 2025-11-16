# código-TCC
Código referente ao Trabalho de Conclusão de Curso de Engenharia de Sistemas.

# Base de dados hibrida:
Os sinais elétricos presentes na base de dados possuem volume muito elevado, onde um único parâmetro, como voltage_hf, pode ultrapassar 400 mil amostras por teste. Armazenar esses sinais diretamente em um banco relacional tornaria o sistema pesado, lento e pouco funcional.
Por isso, adotou-se uma abordagem híbrida: os metadados são armazenados em SQLite, enquanto os sinais permanecem no arquivo HDF5. A ligação entre ambos é feita pelo atributo hdf5_path, que indica a localização exata de cada sinal no HDF5.
Essa estrutura permite consultas rápidas via SQL e garante que apenas os sinais elétricos necessários sejam carregadas durante a análise, evitando a leitura completa do arquivo e tornando o processo mais eficiente.

# Instruções:  
  1. Baixe o arquivo "hif_vegetation_dataset.h5" no kaggle: https://www.kaggle.com/datasets/dougpsg/highimpedance-vegetation-fault-data-set (Dataset disponibilizado por Douglas Gomes por meio do artigo *"VeHIF: An Accessible Vegetation High-Impedance Fault Data Set Format"*.)
  2. Adicione o arquivo "hif_vegetation_dataset.h5" dentro da pasta "data/".
  3. Execute o script "etl_metadata.py" ele irá gerar o banco SQLite "hif_vegetation_data.db". Caso já exista um banco na pasta e você queira criar outro, basta apagá-lo.

# Estrutura:
```
codigo_TCC/
├── data/
│   ├── hif_vegetation_dataset.h5  # HDF5 existente
│   └── hif_vegetation_data.db     # Banco de dados SQLite criado após execução etl_metadata.py
├── script/
│   └── etl_metadata.py            # Cria o banco de dados e popula ele com dados extraídos do arquivo HDF5
└── README.md                      # Documentação
```

# Exemplo de uso:
A seguir, um exemplo simples de consulta SQL no banco de dados SQLite criado:

```python
import sqlite3

conn = sqlite3.connect("data/hif_vegetation_data.db")
cursor = conn.cursor()

cursor.execute("""
    SELECT test_id 
    FROM tests 
    WHERE report_validity='valid' AND max_current>5;
""")

for row in cursor.fetchall():
    print(row)
```



