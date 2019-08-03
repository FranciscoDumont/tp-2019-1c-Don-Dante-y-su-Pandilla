#!/bin/bash
cd "$(dirname $0)/" #me muevo a donde esta este script
cp -v ./configuraciones/base/knl01.cfg ../KNL/
cp -v ./configuraciones/base/lfs01.cfg ../LFS/
cp -v ./configuraciones/base/mem01.cfg ../MEM/
cp -v ./configuraciones/base/mem02.cfg ../MEM/
mkdir -p ../LFS/lfs-base
mkdir -p ../LFS/lfs-base/Bloques
mkdir -p ../LFS/lfs-base/Metadata
mkdir -p ../LFS/lfs-base/Tables
cp -v ./configuraciones/base/Metadata.bin ../LFS/lfs-base/Metadata/
