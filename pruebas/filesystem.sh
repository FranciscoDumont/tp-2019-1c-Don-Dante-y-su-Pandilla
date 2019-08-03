#!/bin/bash
cd $(dirname $0) #me muevo a donde esta este script
cp -v ./configuraciones/filesystem/knl01.cfg ../KNL/
cp -v ./configuraciones/filesystem/lfs01.cfg ../LFS/
cp -v ./configuraciones/filesystem/mem01.cfg ../MEM/
cp -v ./configuraciones/filesystem/mem02.cfg ../MEM/
cp -v ./configuraciones/filesystem/mem03.cfg ../MEM/
mkdir -p ../LFS/lfs-compactacion
mkdir -p ../LFS/lfs-compactacion/Bloques
mkdir -p ../LFS/lfs-compactacion/Metadata
mkdir -p ../LFS/lfs-compactacion/Tables
cp -v ./configuraciones/filesystem/Metadata.bin ../LFS/lfs-compactacion/Metadata/
