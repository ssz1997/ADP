PGDMP     0            	    	    x        	   Q1Hard500    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    41323 	   Q1Hard500    DATABASE     i   CREATE DATABASE "Q1Hard500" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "Q1Hard500";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    41324    A    TABLE     -   CREATE TABLE public."A" (
    "A" integer
);
    DROP TABLE public."A";
       public         postgres    false    3            �            1259    41327    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    41330    B    TABLE     -   CREATE TABLE public."B" (
    "B" integer
);
    DROP TABLE public."B";
       public         postgres    false    3            *          0    41324    A 
   TABLE DATA               "   COPY public."A" ("A") FROM stdin;
    public       postgres    false    196   5
       +          0    41327    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    197   �
       ,          0    41330    B 
   TABLE DATA               "   COPY public."B" ("B") FROM stdin;
    public       postgres    false    198   1       *   �   x��YDQ��#�ҽ^ƿ���C8AR4�rȐ��@�
5��p�_���/���?��2�$�hb�%�4R��o1�"�rɣ��TP����Z�h�E;tҏ�顗>�1��L1Og�e�5V���&[l��v���\p���p���ǰ+�      +   D  x�-�ˑ-9D��B�|y��1'��Qt�� �7r�3��>�Zn��}vs�1o]��^�wl�Uno���9��:�+����_�,���k�<+k��k[�
W˽m犴�e�,(��>*VW�륝\ym>�TaA����Q�����[��V�ޖ����^���.�[ƺ���5߫��@q�=���"}�z��S�o����>����Swhl�8̣k���pa24�NX�)(�TV�AP��������P�Z-D�k��g�T\�?@��#��
�������Cj�/��h���@\���t�	����-�yu�B*�t���[�-�<�Cz�R`k���g��̽��T���:��%�����e�sA�1�lz��J��>i+;�X�B�4@�)���%�����x3 F�d19�����ʡ�����Qϧ�._�ĸ[���2�%$S��a�D PP�;I�p��X�=��$��'"�N ��gb!�>�[���f��MO$ �l���z@ς��|ޓڑ��=�8Vv[
T�T���rb������F��xJ
���iIn�/��"���ycrB�0#⽟�d��~*�Zy�����}�b_ �pI�k@�	Gv	�\0U��=��r�-N�A�[p����3���T���]��z<!���r�Ү�H���Y�u?a*����͉��b����H1{�}V��P:�g�\�"*&[���Ɯ
Zy1ᆠU!Ȏ���S��2�w��t�j-���D7�1Z��W�),�]aM?�iI2bԛ����1RŠ"ۧf���f��3����Ra(ՆN����#�~?�
 1���f�?�      ,   �   x��YDQ��#�ҽ^ƿ���C8AR4�rȐ��@�
5��p�_���/���?��2�$�hb�%�4R��o1�"�rɣ��TP����Z�h�E;tҏ�顗>�1��L1Og�e�5V���&[l��v���\p���p���ǰ+�     