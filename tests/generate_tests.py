import os

SRC_DIR = "../src"
TEST_DIR = "."  # Puisqu’on est déjà dans le dossier tests

def ensure_dir(path):
    """Créer le dossier si nécessaire."""
    os.makedirs(path, exist_ok=True)

for root, _, files in os.walk(SRC_DIR):
    for file in files:
        if file.endswith(".py") and file != "__init__.py":
            src_file_path = os.path.join(root, file)

            # Ex: ../src/module/submodule/fichier.py => module.submodule.fichier
            relative_path = os.path.relpath(src_file_path, SRC_DIR)
            module_parts = relative_path.replace(".py", "").split(os.sep)

            # Nom du module pour import (ex: src.users.manager)
            import_path = ".".join(["src"] + module_parts)

            # Nom du fichier de test : test_<nom_du_fichier>.py
            test_filename = f"test_{module_parts[-1]}.py"

            # Dossier de test cible à l'intérieur de tests/
            test_subdir = os.path.join(TEST_DIR, *module_parts[:-1])
            ensure_dir(test_subdir)

            # Chemin complet du fichier de test
            test_file_path = os.path.join(test_subdir, test_filename)

            # Créer le fichier de test s’il n’existe pas encore
            if not os.path.exists(test_file_path):
                with open(test_file_path, "w") as f:
                    f.write(f"""import pytest
from {import_path} import *

def test_placeholder():
    assert True  # Remplace avec de vrais tests
""")
                print(f"[✓] Créé : {test_file_path}")
            else:
                print(f"[i] Existe déjà : {test_file_path}")
