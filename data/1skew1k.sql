PGDMP     !            	    	    x        	   1skew1000    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    49750 	   1skew1000    DATABASE     i   CREATE DATABASE "1skew1000" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
    DROP DATABASE "1skew1000";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    49751    A    TABLE     -   CREATE TABLE public."A" (
    "A" integer
);
    DROP TABLE public."A";
       public         postgres    false    3            �            1259    49757    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    49754    B    TABLE     -   CREATE TABLE public."B" (
    "B" integer
);
    DROP TABLE public."B";
       public         postgres    false    3            *          0    49751    A 
   TABLE DATA               "   COPY public."A" ("A") FROM stdin;
    public       postgres    false    196   5
       ,          0    49757    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    198   �       +          0    49754    B 
   TABLE DATA               "   COPY public."B" ("B") FROM stdin;
    public       postgres    false    197   �       *   V  x���1��?�Qq�>��@���46��d�9\�HH�N�d�M�T�BթAMjQ�:ԥ7z�>��}�}��2#�b������،ø������M�bn�a^Vc�U��,�w�6�.���.vg�d�;�}ؗ�8��sgrǿ=��m�p�۹�;����b<9�M��H��i�Di�4Y��?����ݳ{xO���S�ם�1*F��!�d����/�;9�g���4�FԨY3^1w�F�hq�n䍾8
g���D�ʑ9:G�(��u��z7�N��#y4��Q=�G��w,�?�G���1Aλ*wV�b�"��)b�#��}����� ?�l'      ,   -  x�-�ٵ�0D������ry��1u��0��T�����9zRO�yz�@��yt,,��ŀE�f�^&��n�L&1�mɈ�-iK&M:�����(�+�-o�I��.v�����b��x�}t<f~>c_�X���,}�>�\��Җ��F������˖vδc�=3�-��i��m��������G{Z�7��9��3�s3m�����^;r�Ӿ��^�k�_���z��Q��v�|�����Yz8�R��y��-�l��Ţk�+�aa�'�ύ���ly%��THOu��1��}���Z����"����~��#�OÄ�U#m8�'����t��}Hj����߾�m�m����o�Y��
������I��Ꮑ=} =|&C��jc�Ҵ,�?��Sx0��q����_�!��<�� Ǘ*ɷ<ķ�����+�l�zR���V�2m���ɋѳ���{0c���">t<�hQ�����q� g'S�V����%8x8A�i��d���W��U:��I�;�q��ӹ��X����K�X߁
�G�X��Tx��P��:yՇG!P�N�|����k~/{����HK��VE�5R]g��/�o�n^5�����|ŧm<|���Ǫ�gy��3v����
��b|,�����ci,����~��!�.7E��<�,�j��L��r�*�j�������I�/Զ$�@�M��t����tw'�n
T��>&"°���0)F3J4�F3B|�>���b�ig%��<n��"yf�����8o�+�l��$oC���}>���_/��yA�pD��)�y�:�jf�W�*�)u���"��)QڎԱ�m��Է��ѫ�G
:8=A@W�7����J�
&p�J�JWq˒Z4\t���sD��^�u�3���R���;�7l;c�Ff��Y{��v�6�l�����ƥ�q�dbKf����l�������]d ��j
��!}�(P��p��-	j�����<��p��9�X(!�vI�~���qT:a9٘���E1uʵǹ�vc�y.��4A ��$��_,�:(�I����1�䮀���M�9�%�a2�oHJ�=�,)�L���8��J-)\�����d����.��2ب��+�9�R�f�Y=��g�d�GG�[9`
h��4��Z��jT(M���ֳK�Q�Ib\W#�%ą(�7���ndY��W�G=����}]��j�m��5ۿ�5/��e:�`�{���0Ǌ�
��,�m���=�n"$����qc_��� |*	���Af������0)����ʅW@�a�_���)T��Kq?4�� �nW�d7l0���4Ɗʕ�Бrn�])LR��Q��ٷ{��XpE�[���d��@@?��(kX�|��*�J��jwĄ�����y2�U؊,P�s-�x����*�;��0�Z�iv]�<��T��t$c�%�
n�B����@���A��qs��w&��V�F��:����KGknRT_>)����p��nmZ�Fn8P�����ſ߿��[���(���?e�	U!N�@���9�������~���:a      +   V  x���1��?�Qq�>��@���46��d�9\�HH�N�d�M�T�BթAMjQ�:ԥ7z�>��}�}��2#�b������،ø������M�bn�a^Vc�U��,�w�6�.���.vg�d�;�}ؗ�8��sgrǿ=��m�p�۹�;����b<9�M��H��i�Di�4Y��?����ݳ{xO���S�ם�1*F��!�d����/�;9�g���4�FԨY3^1w�F�hq�n䍾8
g���D�ʑ9:G�(��u��z7�N��#y4��Q=�G��w,�?�G���1Aλ*wV�b�"��)b�#��}����� ?�l'     