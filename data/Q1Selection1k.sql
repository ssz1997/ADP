PGDMP         	        	    	    x           Q1Selection1000    10.12    10.12     /           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            0           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            1           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            2           1262    33116    Q1Selection1000    DATABASE     o   CREATE DATABASE "Q1Selection1000" WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
 !   DROP DATABASE "Q1Selection1000";
             postgres    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
             postgres    false            3           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                  postgres    false    3                        3079    13241    plpgsql 	   EXTENSION     ?   CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
    DROP EXTENSION plpgsql;
                  false            4           0    0    EXTENSION plpgsql    COMMENT     @   COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
                       false    1            �            1259    33117    AB    TABLE     ?   CREATE TABLE public."AB" (
    "A" integer,
    "B" integer
);
    DROP TABLE public."AB";
       public         postgres    false    3            �            1259    33120    BC    TABLE     ?   CREATE TABLE public."BC" (
    "B" integer,
    "C" integer
);
    DROP TABLE public."BC";
       public         postgres    false    3            �            1259    33123    CDE    TABLE     Q   CREATE TABLE public."CDE" (
    "C" integer,
    "D" integer,
    "E" integer
);
    DROP TABLE public."CDE";
       public         postgres    false    3            *          0    33117    AB 
   TABLE DATA               (   COPY public."AB" ("A", "B") FROM stdin;
    public       postgres    false    196   �
       +          0    33120    BC 
   TABLE DATA               (   COPY public."BC" ("B", "C") FROM stdin;
    public       postgres    false    197   �       ,          0    33123    CDE 
   TABLE DATA               .   COPY public."CDE" ("C", "D", "E") FROM stdin;
    public       postgres    false    198   �       *   =  x�%�ɕ1��`��\~�q�$.n/�Y�t�1?�_���F~c}q���|��j�i�/��刐H@�Af���C͸R�a����8P��a %����-9{q�C�3@�=R�2��`�7G��G��3G^E�TY<[S>.��SR��-_�ߺ����*��w0�޴��%�����������8��8��I9��-�c/�U��}g���u�o(�Iܔ�]���><J�ex�>�M�)��s��|�E;��]K<I�$�y�����Q�)�FmVG1�U>걠�*Ĝ*IL�0&T�i^b./�'���+L�3]�t;�<$$ty��&?�"B�����!�F�4Bֈ��#G��'GY
��Q!I�B� L4�m ]� P�'�p��Q������*}`	��ˉ��]�WG7.�[~>A��D�d�|QՀ	Sx�}d�;�C��2A������Q���AFI��
ɛ�~�7��
����hHH�Q���~�A�$�Y�6H�%�.>��l�u%_g� R�� ���y��:)��*e��@u)ɢ�/�P��4��i�xd�2�n2�v@�f$$�ɼ-��	)q	��IH��.%�H�@j�<�� N�B S	��@v���y�pgq�h�Hg�Q���M� �Ҁt  �ZzTXn�X��tTR��K4`�gaus]�Jy�tVG�v1����I����<B��c9��a�	�Gun�?���@J�D�q!)�ן�P�PR�P*7�D�dR�n��|����ÏϿ&J�鋻)����@�n!�Z���r��$�<)uJ����B&u�>��������1�      +   �  x�-��q0�s\LF�����:",}PNZOl>���w�Ng�3�Y��w�;7wZ��rU��\Vn+ו�J�Na>;�S8�S8�S8�S�=E��I�S�=EO�S�#�H1R�A��b�)F��b��)f��b�S�3�L1S�+�J�R���)�J�R�;�N�S�;�N��iS�;�IqR�'�IqR��i�8)n��⦸)n��⦸).�	2��ClL�1���lL�1�F�?m(E��RPp  �e��� 8�!H���!d�!t��!��0"�%��p"�)�E�,B��"�0B� #�4Z�^hq#�9���#�=���]��B@$	F�$$	J:�Ri�$8	O�%H	S�P�[ϼ�9Wƕqe\Wƕqe\WV-	Z\Wƕqe\Wƕk�Ԃ�n��1�dj�Ԛ�=S�Wƕq�^�Wƕqe\Wƕqe\W��hqe\Wƕq����|���&      ,   D  x�=�[�e)E���t��\j�����QQ�EDp{�Z?���G��1��0Z�l}��.��&��.��M���S�6��g����;4�Ca�=8n���i)m�ʿZ��E\0rh&[Һ�V�к$,XC-���ت�o[�;�v�5q'^%�s�̶hi�20yw-]M;a����M���2`i�̀�4���OP��tL;f��ǆ�qym�����=&��CD�5�}�J�i����s��w=���?�6o��v��}ފ��Jh�8l�Mѱ`����Y�>��Jyax)����+v�]���=�����dWxm�T���ϻ
W���K��MQsp�O[��hf��ϩh9�&_*U���92���.*�05xܝx"���~�|�{*�u�xl2�΂�˪�Q��������~%�rQ�`�WA��Gx�k|	�N��p`�B����("�EK)Ȭ� 0�	�ж�&!��٫��]� l��`�m׫N3��-$��z��2���̊�.�$L�v�|I�y$̺-��j��0L.R���|�@ ��1`�uя��_ C{ �f��+��,����,h^���	�	��B� DЕ��?KI�_HV6�~�ŧ���^t"o=|&I��x"�$�:$w�N���]$��j@:-�A�r���1G�/)j�ɢ���7�����+vKBj� �Eb�+�$���������\�����,�[�t\�1��M \Wq��n��YT��%�܆��J�0	�E�7�l��'�mX����e�k�m{۩�יv) Z��^��nK/��O�z� ��o���Vq�n���#{U�Z95X*e;�Ul����ZϠ [r��kjB�Ғ ��W��~� �ia�M������/���x�8T9%t'�`�TF	���W�\�~s�Z�*�`�ۭ0���v#�{	���q�A\-��(����:� �G� �_��<�j'�J��b�����޶lծ4 �H��@�J��'�ÂT��@��Y)*D��v���꫞�b���������d���A������Q�Յ����,���K�+!�ɓ��?�*�IX���J���l�Ⱥ���� �^={���`5,@���n&Xu�*�$P=�:nL��j���zь�$�`U51��h���kITu�zYKPn��4�QL��Ԑ٫�UK�۱�z#I���7ɶV6��U�J��}�e���~��	T�s������Y]�	Z��v]z��NZ�8��=�����O��$���N�OZ�ۮ7A��R�j��I@��`�����$`������e'��a���I�U��"3�/^XW�6��G����v4YZ���zj k���!YY�,��IQD����<�Ȓ��"�:��b3ū��z��g�2�e�B:�&몪c���#bF��j��:���"y|�w_�� ���CR���^�$�����l㫺7j}+�@U)*���
Pe� �>��z�jz�r`6�������AXֈ^}�Pe���:�:�Z�gV��,�v qU��U�7�d�k�q�o���ܨ �u{�zae�����a�b�~����>�z�f���߭,��˟�Cr����￿�����bh     