PGDMP     1            	    	    x        	   Q1Hard200    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    41282 	   Q1Hard200    DATABASE     i   CREATE DATABASE "Q1Hard200" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "Q1Hard200";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    41283    A    TABLE     -   CREATE TABLE public."A" (
    "A" integer
);
    DROP TABLE public."A";
       public         postgres    false    3            �            1259    41286    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    41289    B    TABLE     -   CREATE TABLE public."B" (
    "B" integer
);
    DROP TABLE public."B";
       public         postgres    false    3            *          0    41283    A 
   TABLE DATA               "   COPY public."A" ("A") FROM stdin;
    public       postgres    false    196   5
       +          0    41286    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    197   �
       ,          0    41289    B 
   TABLE DATA               "   COPY public."B" ("B") FROM stdin;
    public       postgres    false    198   �       *   K   x��A�0����DR��\�G`w!L��K3h!!���
tQ����8x��_�x�""�I�C.i2���XI      +   1  x�5Rɑ�0{C1;��{���XI�~<F"�Q'}���,D��3w�I�hdE����v�OHē���G�u��
:o`��'�wܨΦ�Нu�Q�{r�����B}q����QH+<��Owm��Kݪ�5ͷm�zf�	�a�;.OҌO�^�-��m[�>=Oy�tBܐ���,%$��9'W�)���|����M��:!��NڙRڱ��pEB7-�mO�BKp��Zb(���ֶ��&[Up�I)�d��Z�g�Zvj�5m�ҘS���f���)��xK��-��ѻs����:�`4�S��ߟ��;�]�      ,   K   x��A�0����DR��\�G`w!L��K3h!!���
tQ����8x��_�x�""�I�C.i2���XI     