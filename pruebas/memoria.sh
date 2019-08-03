#!/bin/bash
cd $(dirname $0) #me muevo a donde esta este script
cp -v ./configuraciones/memoria/knl01.cfg ../KNL/
cp -v ./configuraciones/memoria/lfs01.cfg ../LFS/
cp -v ./configuraciones/memoria/mem01.cfg ../MEM/
mkdir -p ../LFS/lfs-prueba-memoria
mkdir -p ../LFS/lfs-prueba-memoria/Bloques
mkdir -p ../LFS/lfs-prueba-memoria/Metadata
mkdir -p ../LFS/lfs-prueba-memoria/Tables
cp -v ./configuraciones/memoria/Metadata.bin ../LFS/lfs-prueba-memoria/Metadata/
