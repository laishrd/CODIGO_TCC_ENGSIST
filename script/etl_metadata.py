import sqlite3
from pathlib import Path
import h5py

def create_database_schema(db_path: Path):

    
    db_name = "hif_vegetation_data.db"
    db_dir = Path("data")
    db_dir.mkdir(exist_ok=True)
    db_path = db_dir / db_name

    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS tests(
            test_id INTEGER PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            fault_type VARCHAR(50), 
            max_current FLOAT,
            report_validity VARCHAR(10) NOT NULL CHECK (report_validity IN ('valid', 'invalid')),
            hdf5_path VARCHAR(255) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS calibrations(
            cal_id INTEGER PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            cal_type VARCHAR(50) NOT NULL CHECK (cal_type IN ('phase-to-phase', 'phase-to-earth')),
            hdf5_path VARCHAR(255) NOT NULL
        );
    """)


    conn.commit()  
    conn.close()   
    print(f"Banco de dados criado em {db_path.absolute()}")



def get_hdf5_attrs(hdf, path):
    try:
        group = hdf[path]
        return dict(group.attrs)
    except KeyError:
        return None

def extract_and_load_data(db_path: Path):

    HDF5_FILE = Path(__file__).parent.parent / 'data' / 'hif_vegetation_dataset.h5'  

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
        
    try:
        with h5py.File(HDF5_FILE, 'r') as hdf:
         
            if '/test' in hdf:
                for test_id in hdf['/test']:
                    try:
                        attrs = get_hdf5_attrs(hdf, f'/test/{test_id}')
                        if not attrs:
                            continue

                     
                        try:
                            if isinstance(attrs['max_current'], (str, bytes)) and attrs['max_current'].decode('utf-8') == 'Empty':
                                max_current = None  
                            else:
                                max_current = float(attrs['max_current'][0]) if hasattr(attrs['max_current'], '__iter__') else float(attrs['max_current'])
                        except (ValueError, TypeError, KeyError):
                            max_current = None 

                      
                        required_attrs = ['filename', 'report_validity']
                        missing_attrs = [attr for attr in required_attrs if attr not in attrs]
                        if missing_attrs:
                            print(f"Teste {test_id} está faltando o(s) seguinte(s) atributo(s) obrigatório(s): {missing_attrs}. Teste não inserido.")
                            continue

                        cursor.execute("""
                            INSERT INTO tests(
                                test_id, filename, fault_type, max_current, report_validity, hdf5_path
                            ) VALUES (?, ?, ?, ?, ?, ?)        
                        """, (
                            int(test_id),
                            str(attrs['filename']),
                            str(attrs['fault_type']) if 'fault_type' in attrs else None,
                            #str(attrs['max_current']) if 'max_current' in attrs else None,
                            max_current,
                            str(attrs['report_validity']),
                            f'/test/{test_id}'
                        ))

                    except Exception as e:
                        print(f"Erro no teste {test_id}: {str(e)}")
                        continue

            
            if '/cal' in hdf:
                for cal_id in hdf['/cal']:
                    try:
                        attrs = get_hdf5_attrs(hdf, f'/cal/{cal_id}')
                        if not attrs:
                            continue

                       
                        required_attrs = ['filename', 'cal_type']
                        missing_attrs = [attr for attr in required_attrs if attr not in attrs]
                        if missing_attrs:
                            print(f"Calibração {cal_id} está faltando o(s) seguinte(s) atributo(s): {missing_attrs}. Calibração não inserida.")
                            continue

                        cursor.execute("""
                            INSERT INTO calibrations(
                                cal_id, filename, cal_type, hdf5_path
                            ) VALUES (?, ?, ?, ?)
                        """, (
                            int(cal_id),
                            str(attrs['filename']),
                            str(attrs['cal_type']),
                            f'/cal/{cal_id}'
                        ))

                    except Exception as e:
                        print(f"Erro na calibração {cal_id}: {str(e)}")
                        continue
               
        conn.commit()
        print("Os dados foram extraídos e salvos no banco de dados!")
    
    except Exception as e:
        conn.rollback()
        print(f"Erro durante a extração {str(e)}")

    finally:
        conn.close()



if __name__ == "__main__":
    db_path = Path("data/hif_vegetation_data.db")
    create_database_schema(db_path)
    extract_and_load_data(db_path)
