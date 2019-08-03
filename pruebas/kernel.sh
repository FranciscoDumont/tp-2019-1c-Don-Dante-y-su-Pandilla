#!/bin/bash
cd $(dirname $0) #me muevo a donde esta este script
cp -v ./configuraciones/kernel/knl01.cfg ../KNL/
cp -v ./configuraciones/kernel/lfs01.cfg ../LFS/
cp -v ./configuraciones/kernel/mem01.cfg ../MEM/
cp -v ./configuraciones/kernel/mem02.cfg ../MEM/
cp -v ./configuraciones/kernel/mem03.cfg ../MEM/
cp -v ./configuraciones/kernel/mem04.cfg ../MEM/
mkdir -p ../LFS/lfs-prueba-kernel
mkdir -p ../LFS/lfs-prueba-kernel/Bloques
mkdir -p ../LFS/lfs-prueba-kernel/Metadata
mkdir -p ../LFS/lfs-prueba-kernel/Tables
cp -v ./configuraciones/kernel/Metadata.bin ../LFS/lfs-prueba-kernel/Metadata/
