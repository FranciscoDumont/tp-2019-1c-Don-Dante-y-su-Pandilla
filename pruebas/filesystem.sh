#!/bin/bash
cp ./configuraciones/filesystem/knl01.cfg ../KNL/
cp ./configuraciones/filesystem/lfs01.cfg ../LFS/
cp ./configuraciones/filesystem/mem01.cfg ../MEM/
cp ./configuraciones/filesystem/mem02.cfg ../MEM/
cp ./configuraciones/filesystem/mem03.cfg ../MEM/
mkdir -p /home/utnso/lfs-compactacion
cp ./configuraciones/filesystem/Metadata.bin /home/utnso/lfs-compactacion/
