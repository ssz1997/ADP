PGDMP                 	    	    x        	   Q1Hard100    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    33146 	   Q1Hard100    DATABASE     i   CREATE DATABASE "Q1Hard100" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "Q1Hard100";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    33147    A    TABLE     -   CREATE TABLE public."A" (
    "A" integer
);
    DROP TABLE public."A";
       public         postgres    false    3            �            1259    33150    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    33153    B    TABLE     -   CREATE TABLE public."B" (
    "B" integer
);
    DROP TABLE public."B";
       public         postgres    false    3            *          0    33147    A 
   TABLE DATA               "   COPY public."A" ("A") FROM stdin;
    public       postgres    false    196   5
       +          0    33150    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    197   r
       ,          0    33153    B 
   TABLE DATA               "   COPY public."B" ("B") FROM stdin;
    public       postgres    false    198          *   -   x��I   �?a��.��!T�tôlǕ*�&]�LY����
>�u      +   �   x�-�� CϤ����e��c܋�^W$���
n�#�Ab�v���qq���Ԃ��G��iFL��ycٱ� z�V��7[phhjC���(�-
�;�{�.��ђ�e<��;�-���T�r��Ki�8���1q�=R!��J|? ~�(�      ,   -   x��I   �?a��.��!T�tôlǕ*�&]�LY����
>�u     