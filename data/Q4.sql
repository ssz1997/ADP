PGDMP     %            	    	    x           414q4    10.12    10.12     4           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            5           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            6           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            7           1262    25100    414q4    DATABASE     e   CREATE DATABASE "414q4" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "414q4";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            8           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            9           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    25137    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    25140    BC    TABLE     ?   CREATE TABLE public."BC" (
    "B" integer,
    "C" integer
);
    DROP TABLE public."BC";
       public         postgres    false    3            �            1259    25125    EF    TABLE     ?   CREATE TABLE public."EF" (
    "E" integer,
    "F" integer
);
    DROP TABLE public."EF";
       public         postgres    false    3            �            1259    25128    FG    TABLE     ?   CREATE TABLE public."FG" (
    "F" integer,
    "G" integer
);
    DROP TABLE public."FG";
       public         postgres    false    3            0          0    25137    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    198   �       1          0    25140    BC 
   TABLE DATA               (   COPY public."BC" ("B", "C") FROM stdin;
    public       postgres    false    199   �       .          0    25125    EF 
   TABLE DATA               (   COPY public."EF" ("E", "F") FROM stdin;
    public       postgres    false    196   u       /          0    25128    FG 
   TABLE DATA               (   COPY public."FG" ("F", "G") FROM stdin;
    public       postgres    false    197          0   �   x�-��� !C�R�>N/���=�!|��CQ���5-w����o褩rd���C]|��ֶ�x���s�=�H�i���y����������{��K��?Mb߆+��ey�r�����}t{Q�>�P��yu�r����~Vp��w������w�pl����=�KL��������x�����tˇ����V�Y��t9C�}�!O��;�3�]}6�͙~:{�~��}=Ku      1   �   x�5��C1C�0L��d��?G���#?��m��{-��,#қa��z�n�H?�E�oƙ�xa[HOq�3�w8Z�����W�WlC���?�|��p����F�^���ʭVο�O�|���u]�:�!�14R;K�L����w��q�=�+�      .   �   x�5��1C�PL�q/鿎�8{�?}�[��V/A���P�K�V8Ohm�?��H�R4�[A��Rf�1ɓCHucn�_�������|��p'���,ɾ
�1���h���Í���}}wG�^z���y{���Q�/+�      /   �   x�-��1�"���������ˡ��ȳ������V��O"�c~�$��ky�����-^o8��{8���+���9�!o�/"�^�#ߑ�pC%L��6���Ї2��<����(�������3ԩ���=5�y\�Q�g�5a��,��xW�O��F|c��zߏ�� v/<�     