
import subprocess
from ingestao_copy import ingestao 
from transformacao_copy import transformacao 
from carga_copy import carga
def run_pipeline():
    print("🚀 Iniciando execução do pipeline...")
    data = ingestao()
    print("Ingestão executado com sucesso")
    data = transformacao()
    print("Transformacao executado com sucesso")
    data = carga()
    print("🏁 Pipeline executado com sucesso!")

if __name__ == "__main__":
    run_pipeline()