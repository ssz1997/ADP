PGDMP                 	    	    x        	   Q1Hard400    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    41313 	   Q1Hard400    DATABASE     i   CREATE DATABASE "Q1Hard400" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "Q1Hard400";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    41314    A    TABLE     -   CREATE TABLE public."A" (
    "A" integer
);
    DROP TABLE public."A";
       public         postgres    false    3            �            1259    41317    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    41320    B    TABLE     -   CREATE TABLE public."B" (
    "B" integer
);
    DROP TABLE public."B";
       public         postgres    false    3            *          0    41314    A 
   TABLE DATA               "   COPY public."A" ("A") FROM stdin;
    public       postgres    false    196   5
       +          0    41317    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    197   �
       ,          0    41320    B 
   TABLE DATA               "   COPY public."B" ("B") FROM stdin;
    public       postgres    false    198   `       *      x��I1��3����c��|AR4�Y.:H(P�B�-��!�&�$�hbK\�"2�"��䒗:��������2�ԥ-:�_P�C�^�2�L2ż�a�,s��n��/��>���"i      +   �  x�5�K$9C�p�	c�Iߥ��y��7YI����wZ.��N���y���-�cE<�|^a7��7��;,����|[?'/=����N�.��z��$���4�?�����dh�!��leO^ wT��KcN�g��ě����eI�������s�0��"a}�����d@���p?�q�Zϳ��!<PeC��xo��5��T���҄�M�ZBD���bKޘq	�P��oF&�%5:���;�]/�WZ�����l�TJ�R���)Eܷҗ��]D��i!�k
�v�~p���-0.��@�V L�X���F�&�<S�$J� <��Q��>3����?=��*�A�R_6w[�h��}O:^A�ó�ݓ�)��ƥ C%}����Y�GF�А����ṁr��D���eZ(�Pz�oL;vQ�h˥�;���e,�sf�w���%�%����#%��Ã��w���;�Vg��ԸguD-Gvy�c�\4�~�4��H�Ot���='��N:sK�qѝ�ԎF�Q��! ��P�����ٚ#X��c����Y��}d��i!�z�W�[㿫;�!>g�.�(��'<�����(�#[<m#�u'"\{�dt�]@��)2kv����(�RC�������.���m��       ,      x��I1��3����c��|AR4�Y.:H(P�B�-��!�&�$�hbK\�"2�"��䒗:��������2�ԥ-:�_P�C�^�2�L2ż�a�,s��n��/��>���"i     