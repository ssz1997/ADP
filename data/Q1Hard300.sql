PGDMP                 	    	    x        	   Q1Hard300    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    41303 	   Q1Hard300    DATABASE     i   CREATE DATABASE "Q1Hard300" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "Q1Hard300";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    41304    A    TABLE     -   CREATE TABLE public."A" (
    "A" integer
);
    DROP TABLE public."A";
       public         postgres    false    3            �            1259    41307    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    41310    B    TABLE     -   CREATE TABLE public."B" (
    "B" integer
);
    DROP TABLE public."B";
       public         postgres    false    3            *          0    41304    A 
   TABLE DATA               "   COPY public."A" ("A") FROM stdin;
    public       postgres    false    196   5
       +          0    41307    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    197   �
       ,          0    41310    B 
   TABLE DATA               "   COPY public."B" ("B") FROM stdin;
    public       postgres    false    198   �       *   d   x��GqA ����vc��?-�2KH�����X"D�-Flq�O.�1)K���W>�T�OY�ը�����C����Go}���,&M�oh�6�\�����E      +   �  x�5�ɕ 1C��<�yɥ�c$U��.#��m�<��z_��	��6��Z�d���I^�����N��H��o/����oT=�B:TFUn{c�>i��ؤoK�f��a�����m4؆9/>Mv�w�^Q#�U{%U�X���<�'�7.��xmm>�P��Q��r�b�
I<��s� ��-m��~�U�`�i�0�j	I/��pmo���ԽI'�嬼� g$v6e8�/0D.����M��9�c��b*����@�h�-",�9r|B��i�1%��Cz�'8_�O��1�I�
lv&�g�HM�ᙙn�A0W�=DPb�Yw�P�rŭ��lspe�����1����u�"�/O���9`9	�k��\� O� p��\  je��Qz5hS G����Q��ؗAbx��׃���&�&8�\,�6�D� |���J�0S�	�E���n m|[��|���s��J��      ,   d   x��GqA ����vc��?-�2KH�����X"D�-Flq�O.�1)K���W>�T�OY�ը�����C����Go}���,&M�oh�6�\�����E     