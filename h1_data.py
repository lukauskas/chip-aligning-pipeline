from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
from genome_alignment import BowtieAlignmentTask


H1_DATASETS = [
    # -- Sample SAMN00002925 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX007379', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # 30CMAAAXX081006-2-S
    {'cell_type': u'H1', 'experiment_accession': 'SRX007389', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me1'},
    # 314G6AAXX090406-7-S
    # -- Sample SAMN00002931 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX007385', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # 30CMAAAXX081006-5-S
    # -- Sample SAMN00002932 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX007386', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9ac'},
    # 30H81AAXX090613-6-S
    # -- Sample SAMN00002933 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX007387', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # 30HCBAAXX090610-8-S
    # -- Sample SAMN00002934 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX007388', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # 30HCBAAXX090610-7-S
    # -- Sample SAMN00002935 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX007390', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # 314G6AAXX090406-5-S
    # -- Sample SAMN00003164 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006262', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # HS0997-1_305H8AAXX
    {'cell_type': u'H1', 'experiment_accession': 'SRX006117', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # HS0996
    {'cell_type': u'H1', 'experiment_accession': 'SRX006116', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9ac'},
    # HS0995
    # -- Sample SAMN00003165 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006263', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # HS0998-1_305Y8AAXX
    # -- Sample SAMN00003166 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006268', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # HS1028-1_3061PAAXX
    # -- Sample SAMN00003167 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006270', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # HS1032-1_305H8AAXX
    # -- Sample SAMN00003168 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006272', 'study_accession': 'PRJNA34535', 'data_track': 'MRE-Seq'},
    # HS1052-1_30WLJAAXX
    # -- Sample SAMN00003169 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006798', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # HS1345-1_305H8AAXX
    # -- Sample SAMN00003170 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006873', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me1'},
    # HS1413-1_42ENEAAXX
    # -- Sample SAMN00003233 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX010885', 'study_accession': 'PRJNA34535', 'data_track': 'MRE-Seq'},
    # HS1153-1_313MYAAXX
    # -- Sample SAMN00003234 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX010888', 'study_accession': 'PRJNA34535',
     'data_track': 'smRNA-Seq'},  # HS1295-1_42ERCAAXX
    # -- Sample SAMN00003235 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX010889', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # HS1346-1_305H8AAXX
    # -- Sample SAMN00003236 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX010891', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # HS1347-1_305H8AAXX
    # -- Sample SAMN00003237 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX010893', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # HS1348-1_305H8AAXX
    # -- Sample SAMN00003239 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX010895', 'study_accession': 'PRJNA34535',
     'data_track': 'MeDIP-Seq'},  # HS1303-1_305H8AAXX
    # -- Sample SAMN00004459 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027883', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX006874', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # renlab.H3K27me3.CDI-01.02
    {'cell_type': u'H1', 'experiment_accession': 'SRX006235', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # renlab.H3K36me3.CDI-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX006236', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me1'},
    # renlab.H3K4me1.CDI-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX006237', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # renlab.H3K4me3.CDI-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX006875', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9ac'},
    # renlab.H3K9ac.CDI-01.02
    # -- Sample SAMN00004461 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006789', 'study_accession': 'PRJNA34535',
     'data_track': 'Bisulfite-Seq'},  # methylC-seq_h1_r1a
    {'cell_type': u'H1', 'experiment_accession': 'SRX006782', 'study_accession': 'PRJNA34535',
     'data_track': 'Bisulfite-Seq'},  # methylC-seq_h1_r1b
    {'cell_type': u'H1', 'experiment_accession': 'SRX007165', 'study_accession': 'PRJNA34535',
     'data_track': 'mRNA-Seq'},  # mRNA-seq_h1_r1
    {'cell_type': u'H1', 'experiment_accession': 'SRX398166', 'study_accession': 'PRJNA34535',
     'data_track': 'smRNA-Seq'},  # smRNA-seq_h1_r1a
    {'cell_type': u'H1', 'experiment_accession': 'SRX007166', 'study_accession': 'PRJNA34535',
     'data_track': 'smRNA-Seq'},  # smRNA-seq_h1_r1
    # -- Sample SAMN00004462 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX006239', 'study_accession': 'PRJNA34535',
     'data_track': 'Bisulfite-Seq'},  # methylC-seq_h1_r2a
    {'cell_type': u'H1', 'experiment_accession': 'SRX006240', 'study_accession': 'PRJNA34535',
     'data_track': 'Bisulfite-Seq'},  # methylC-seq_h1_r2b
    {'cell_type': u'H1', 'experiment_accession': 'SRX006241', 'study_accession': 'PRJNA34535',
     'data_track': 'Bisulfite-Seq'},  # methylC-seq_h1_r2c
    {'cell_type': u'H1', 'experiment_accession': 'SRX398167', 'study_accession': 'PRJNA34535',
     'data_track': 'smRNA-Seq'},  # smRNA-seq_h1_r2a
    # -- Sample SAMN00004698 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027882', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027853', 'study_accession': 'PRJNA34535', 'data_track': 'H3K18ac'},
    # renlab.H3K18ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX012366', 'study_accession': 'PRJNA34535', 'data_track': 'H3K27ac'},
    # renlab.H3K27ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX012368', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # renlab.H3K27me3.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX012371', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # renlab.H3K36me3.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX012373', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me1'},
    # renlab.H3K4me1.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027693', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me2'},
    # renlab.H3K4me2.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX012501', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # renlab.H3K4me3.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027874', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # renlab.H3K9me3.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027879', 'study_accession': 'PRJNA34535', 'data_track': 'H4K5ac'},
    # renlab.H4K5ac.hESC-01.01
    # -- Sample SAMN00006218 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX014392', 'study_accession': 'PRJNA34535',
     'data_track': 'mRNA-Seq'},  # HS1272-1_42VR1AAXX
    # -- Sample SAMN00012247 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX019897', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # 30H81AAXX090613-1-S
    {'cell_type': u'H1', 'experiment_accession': 'SRX019898', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # 30H81AAXX090613-7-S
    {'cell_type': u'H1', 'experiment_accession': 'SRX019894', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me1'},
    # 30E62AAXX090709-5-S
    {'cell_type': u'H1', 'experiment_accession': 'SRX019896', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # 30H81AAXX090613-8-S
    # -- Sample SAMN00012249 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX019895', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # 314G7AAXX090305-5-S
    # -- Sample SAMN00012250 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX019899', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # 42LR0AAXX091002-6-S
    {'cell_type': u'H1', 'experiment_accession': 'SRX019900', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9ac'},
    # 42LR0AAXX091002-5-S
    # -- Sample SAMN00114948 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027884', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027889', 'study_accession': 'PRJNA34535', 'data_track': 'H2AK5ac'},
    # renlab.H2AK5ac.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027844', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK120ac'},  # renlab.H2BK120ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027846', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK12ac'},  # renlab.H2BK12ac.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027848', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK15ac'},  # renlab.H2BK15ac.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027850', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK20ac'},  # renlab.H2BK20ac.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027852', 'study_accession': 'PRJNA34535', 'data_track': 'H2BK5ac'},
    # renlab.H2BK5ac.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027692', 'study_accession': 'PRJNA34535', 'data_track': 'H3K18ac'},
    # renlab.H3K18ac.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027855', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K23me2'},  # renlab.H3K23me2.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027860', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4ac'},
    # renlab.H3K4ac.hESC-01.01
    # -- Sample SAMN00114949 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027885', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-04.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027866', 'study_accession': 'PRJNA34535', 'data_track': 'H3K56ac'},
    # renlab.H3K56ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027878', 'study_accession': 'PRJNA34535',
     'data_track': 'H4K20me1'},  # renlab.H4K20me1.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027881', 'study_accession': 'PRJNA34535', 'data_track': 'H4K91ac'},
    # renlab.H4K91ac.hESC-01.01
    # -- Sample SAMN00114950 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027888', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-07.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX121242', 'study_accession': 'PRJNA34535',
     'data_track': 'DNase hypersensitivity'},  # DS18873
    {'cell_type': u'H1', 'experiment_accession': 'SRX121247', 'study_accession': 'PRJNA34535',
     'data_track': 'DNase hypersensitivity'},  # DS19100
    {'cell_type': u'H1', 'experiment_accession': 'SRX027691', 'study_accession': 'PRJNA34535', 'data_track': 'H2AK5ac'},
    # renlab.H2AK5ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027845', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK12ac'},  # renlab.H2BK12ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027851', 'study_accession': 'PRJNA34535', 'data_track': 'H2BK5ac'},
    # renlab.H2BK5ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027857', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # renlab.H3K27me3.hESC-03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027858', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K36me3'},  # renlab.H3K36me3.hESC-03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027694', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me2'},
    # renlab.H3K4me2.hESC-03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027864', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me3'},
    # renlab.H3K4me3.hESC-03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027870', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K79me2'},  # renlab.H3K79me2.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027872', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9ac'},
    # renlab.H3K9ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX129882', 'study_accession': 'PRJNA34535', 'data_track': 'H4K8ac'},
    # renlab.H4K8ac.hESC.H1.01.01
    # -- Sample SAMN00115027 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027886', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-05.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027847', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK15ac'},  # renlab.H2BK15ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027849', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK20ac'},  # renlab.H2BK20ac.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027869', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K79me1'},  # renlab.H3K79me1.hESC-03.01
    # -- Sample SAMN00115028 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027854', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K23me2'},  # renlab.H3K23me2.hESC-01.01
    # -- Sample SAMN00115029 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027887', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC-06.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027871', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K79me2'},  # renlab.H3K79me2.hESC-02.01
    # -- Sample SAMN00115030 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027861', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4me1'},
    # renlab.H3K4me1.hESC-03.01
    # -- Sample SAMN00115031 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX027867', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K79me1'},  # renlab.H3K79me1.hESC-01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027868', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K79me1'},  # renlab.H3K79me1.hESC-02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027876', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # renlab.H3K9me3.hESC-03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX027877', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # renlab.H3K9me3.hESC-04.01
    # -- Sample SAMN00205530 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX056744', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC.H1.09.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX135209', 'study_accession': 'PRJNA34535', 'data_track': 'H2A.Z'},
    # renlab.H2A.Z.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX095608', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK120ac'},  # renlab.H2BK120ac.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX095609', 'study_accession': 'PRJNA34535',
     'data_track': 'H2BK120ac'},  # renlab.H2BK120ac.hESC.H1.03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX056726', 'study_accession': 'PRJNA34535', 'data_track': 'H3K14ac'},
    # renlab.H3K14ac.hESC.H1.01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX056729', 'study_accession': 'PRJNA34535', 'data_track': 'H3K23ac'},
    # renlab.H3K23ac.hESC.H1.01.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX056734', 'study_accession': 'PRJNA34535', 'data_track': 'H3K4ac'},
    # renlab.H3K4ac.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX056736', 'study_accession': 'PRJNA34535', 'data_track': 'H3K56ac'},
    # renlab.H3K56ac.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX101260', 'study_accession': 'PRJNA34535', 'data_track': 'H3K9me3'},
    # renlab.H3K9me3.hESC.H1.06.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX095612', 'study_accession': 'PRJNA34535',
     'data_track': 'H4K20me1'},  # renlab.H4K20me1.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX081232', 'study_accession': 'PRJNA34535', 'data_track': 'H4K5ac'},
    # renlab.H4K5ac.hESC.H1.03.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX135237', 'study_accession': 'PRJNA34535', 'data_track': 'H4K8ac'},
    # renlab.H4K8ac.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX081233', 'study_accession': 'PRJNA34535', 'data_track': 'H4K91ac'},
    # renlab.H4K91ac.hESC.H1.02.01
    # -- Sample SAMN00205531 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX040606', 'study_accession': 'PRJNA34535',
     'data_track': 'ChIP-Seq input'},  # renlab.Input.hESC.H1.08.01
    # -- Sample SAMN00255377 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX056727', 'study_accession': 'PRJNA34535', 'data_track': 'H3K14ac'},
    # renlab.H3K14ac.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX056730', 'study_accession': 'PRJNA34535', 'data_track': 'H3K23ac'},
    # renlab.H3K23ac.hESC.H1.02.01
    {'cell_type': u'H1', 'experiment_accession': 'SRX056722', 'study_accession': 'PRJNA34535', 'data_track': 'H3K27ac'},
    # renlab.H3K27ac.hESC.H1.03.01
    # -- Sample SAMN00855416 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX142115', 'study_accession': 'PRJNA34535',
     'data_track': 'mRNA-Seq'},  # polyA-RNA-seq_h1_r1a
    # -- Sample SAMN00855417 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX142116', 'study_accession': 'PRJNA34535',
     'data_track': 'mRNA-Seq'},  # polyA-RNA-seq_h1_r2a
    # -- Sample SAMN02232068 -----
    {'cell_type': u'H1', 'experiment_accession': 'SRX322086', 'study_accession': 'PRJNA34535',
     'data_track': 'H3K27me3'},  # renlab.H3K27me3.hESC.H1.02.01

]


class AlignedH1Task(luigi.Task):
    genome_version = luigi.Parameter()
    pretrim_reads = luigi.BooleanParameter(default=True)

    def requires(self):
        for d in H1_DATASETS:
            yield BowtieAlignmentTask(genome_version=self.genome_version,
                                      pretrim_reads=self.pretrim_reads, **d)

    def complete(self):
        return all([x.complete() for x in self.requires()])


if __name__ == '__main__':
    luigi.run(main_task_cls=AlignedH1Task)