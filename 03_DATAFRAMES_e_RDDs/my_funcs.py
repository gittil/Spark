import os


def qual_os():
    """Função para reconhecer qual SO estou usando e definir sozinho o path do arquivo
    """
    operational_system = os.name
    
    if operational_system == "nt":
        path_file = "D:/downloads/despachantes.csv"
    elif operational_system == 'posix':
        path_file = "/home/douglas/download/despachantes.csv"
    else:
        print("não é nada")
    
    return path_file

##----------------------------------------------------------------------------------------------------------


